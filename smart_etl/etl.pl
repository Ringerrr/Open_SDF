#!/usr/bin/perl

use warnings;       # raise warnings when programmers do silly things
use strict;         # don't allow variables to be used without being defined

use Carp;           # Better error messages
use POSIX;          # Perl's built-in POSIX functions
use DBI;            # Perl's database driver infrastructure

use Getopt::Long;   # Libraries to parse command-line arguments
use IO::Handle;     # Libraries for reading / writing to files

use JSON;

#use Text::CSV_XS;   # Library for RAPID csv reading / writing

use SmartAssociates::Base;
use SmartAssociates::Database::Connection::Base;
use SmartAssociates::Database::Connection::Memory; # we use a constant from here even when the rest of the class is not in use

# TODO: dynamic loading of DB Item classes
use SmartAssociates::Database::Item::Base;
use SmartAssociates::Database::Item::Batch::Base;
use SmartAssociates::Database::Item::Job::Base;

# TODO: dynamic loading of Processing Group classes
use SmartAssociates::ProcessingGroup::Base;

use SmartAssociates::Globals;
use SmartAssociates::Iterator;
use SmartAssociates::Log;

use SmartAssociates::WorkCollection::Base;
use SmartAssociates::WorkCollection::Harvest;

use SmartAssociates::TemplateConfig::Base;
use SmartAssociates::TemplateConfig::SQL;
use SmartAssociates::TemplateConfig::ForkChildJob;

$| = 1; # disable output buffering

####################################################
# Assemble some other global variables
my  $log_level_text                 = 'info';               # Set a default log level

my  $nzlog;                                                 # Arg: parse an nzlog

my  $P_EXTRACT_DATE;                                        # Arg: the date ( of export ) of the data we're processing
my  $P_MAX_CONCURRENT               = 10;                   # Arg: a default number of concurrent processes ( that we can spawn - NOT the max total )
my  $P_MAX_ERRORS;                                          # Arg: max number of errors for a single file import
my  $JOB_ID;                                                # Arg: the job to process
my  $P_PARAMETER_RECURSION_LIMIT    = 20;                   # Arg: the maximum recursion limit when resolving parameters

my  $p_processing_group;                                    # Arg: run a process group
my  $p_extract_group;                                       # Arg: run an extract group
my  $p_work_collection;                                     # Arg: dynamically load and run a work collection of a given type

my  $p_migration_batch;                                     # Arg: a migration batch identifier
my  $p_batch_id;                                            # Arg: run a batch process by batch id

my  $p_simulate;                                            # Arg: simulate-run. Don't execute template SQL

my  $p_fifo_base;                                           # Arg: the base of a FIFO ( passed from parent )

my  $user_profile;                                          # Arg: Determines which SQLite config in SMART_CONFIG_BASE that we use

my  $p_args;                                                # Arg: JSON-encoded args

my  $globals = SmartAssociates::Globals->new();

####################################################

# Now we parse the command-line arguments with Getopt::Long
#
#    If we're called with the --job_id arg, we'll try to restart stg processing
#    defined by the given job ( ie we'll try to load from the filename specified
#    in the job again ). This is useful for cases where the load crashed for some
#    reason, for example we exceeded the maximum number of allowed errors in the
#    import. Someone could intervene, clean up the file, and re-start the job with
#    this arg. The other way to restart it would to be rename the file and drop it
#    in the right directory ( then pass --source_system_code ). Note that the job
#    table acts as a locking mechanism that prevents us from otherwise processing
#    the same file twice

GetOptions( 
    'job-id=i'                  => \$JOB_ID
  , 'log-level=s'               => \$log_level_text
  , 'user-profile=s'            => \$user_profile
  , 'max-concurrent=i'          => \$P_MAX_CONCURRENT
  , 'max-errors=i'              => \$P_MAX_ERRORS
  , 'extract-date=s'            => \$P_EXTRACT_DATE
  , 'parse-nzlog=s'             => \$nzlog
  , 'processing-group=s'        => \$p_processing_group
  , 'extract-group=s'           => \$p_extract_group
  , 'simulate'                  => \$p_simulate
  , 'recursion-limit=i'         => \$P_PARAMETER_RECURSION_LIMIT
  , 'fifo-base=s'               => \$p_fifo_base
  , 'work-collection=s'         => \$p_work_collection
  , 'migration-batch=s'         => \$p_migration_batch
  , 'batch-id=i'                => \$p_batch_id
  , 'args=s'                    => \$p_args
);

# Do some checks to make sure we've been called in the correct way
if (
        ! $JOB_ID
     && ! $p_batch_id
     && ! $nzlog
     && ! $p_processing_group
     && ! $p_extract_group
     && ! $p_work_collection
) {

    die( "I was not passed a command-line option to tell me what to do!\n"
       . "Please run with one of the following:\n"
       . "   --job-id=N                            # Attempt to re-run job defined by job_id N\n"
       . "  or\n"
       . "   --process-group=GROUP_NAME            # Execute process group GROUP_NAME\n"
       . "Other options args:\n"
       . "   --log-level=[fatal,error,warn,info,debug]\n"
       . "   --max-concurrent=N                    # The maximum number of concurrent loads per batch ( default is 5 )\n"
       . "   --max-errors=N                        # The maximum number of errors in a single nzload operation before we die\n"
       . "   --nz-delim-workaround                 # Work around a Netezza 7.0.x bug with empty date / time delimiters\n"
       . "   --simulate                            # Simulation only. Don't execute any template SQL\n"
       . "   --recursion-limit                     # Max recursion limit when resolving parameters\n"
       . "   --lucky-dip                           # For the adventurous\n"
    );
    
}

my $log_type;

if ( $JOB_ID ) {
    
    $log_type = "slave_job_" . $JOB_ID;
        
} elsif ( $nzlog ) {
    
    $log_type = "nzlog_parse";
    
} elsif ( $p_processing_group ) {
    
    $log_type = "processing_group_" . $p_processing_group;
    
} elsif ( $p_batch_id ) {
    
    $log_type = "process_group_by_batch_" . $p_batch_id 
    
} elsif ( $p_extract_group ) {
    
    $log_type = "extract_group_" . $p_extract_group;
    
} elsif ( $p_work_collection ) {
    
    $log_type = "work_collection_" . $p_work_collection;
    
}

$ENV{'PGAPPNAME'} = 'Smart Data Framework';

my $dir_separator;

if ( $^O eq "linux" ) {
    $dir_separator = '/';
} else {
    $dir_separator = '\\';
}

$globals->DIR_SEPARATOR( $dir_separator );

# Determine the config base
if ( ! $ENV{'SMART_CONFIG_BASE'} ) {
    if ( $^O eq "linux" ) {
        if ( $ENV{'XDG_CONFIG_HOME'} ) {
            $ENV{'SMART_CONFIG_BASE'} = $ENV{'XDG_CONFIG_HOME'} . "/profiles";
        } else {
            $ENV{'SMART_CONFIG_BASE'} = $ENV{"HOME"} . "/.smart_config";
        }
    } else {
        die( "Missing SMART_CONFIG_BASE environment variable - you MUST set this for non-linux installations" );
    }
}

my $config_base = $ENV{'SMART_CONFIG_BASE'};

# Determine the user profile
if ( ! $user_profile ) {
    if ( $ENV{'SDF_USER_PROFILE'} ) {
        $user_profile = $ENV{'SDF_USER_PROFILE'};
    } else {
        $ENV{'SDF_USER_PROFILE'} = getpwuid($<);
        $user_profile = $ENV{'SDF_USER_PROFILE'};
    }
}

$globals->USER_PROFILE( $user_profile );

# The config folder
my $config_folder = $config_base . '/' . $user_profile ;

# The config path is the full path to SQLite database containing our config 
$globals->SMART_CONFIG_PATH( $config_folder . '/config.db' );

# The log directory sits inside the config folder
my $log_dir = $config_folder . '/etl_logs';

if ( ! -d $log_dir ) {
    mkdir( $log_dir )
        || die( "Can't create log directory: [$log_dir] " . $! );
}

$globals->LOGDIR( $log_dir );

# This is expected to be a JSON-encoded structure
my $args;

if ( $p_args ) {
    $args = decode_json( $p_args );
}

# This is a hack that allows us to disable the FIFO for debugging migrations jobs
if ( $args->{disable_fifo} ) {
    $globals->DISABLE_FIFO( 1 );
} else {
    $globals->DISABLE_FIFO( 0 );
}    

my $log = SmartAssociates::Log->new(
    $globals
  , {
        log_subdir      => undef        # TODO
      , log_type        => $log_type
      , log_level_text  => $log_level_text
    }
);

$globals->LOG( $log );

# This was for debugging DB2 library issues:
#$log->info( "Dumping environment variables ..." );
#
#foreach my $key ( qw {
#    LD_LIBRARY_PATH
#    TD_ICU_DATA
#    COPLIB
#    COPERR
#    TDAT_DBD_CLI_LIB
#    TDAT_DBD_CLI_INC
#} ) {
#    no warnings 'uninitialized';
#    my $log_string = "$key:";
#    if ( $ENV{$key} ) {
#        $log_string .= ( ' ' x ( length($key) - 40 ) )  . " " . $ENV{$key};
#    } else {
#        $log_string .= " <NULL>";
#    }
#    $log->info( $log_string );
#}

eval {
    
    $globals->initialise();

    foreach my $path ( $globals->ALL_ETL_PATHS ) {
        push @INC, $path;
    }

    # We support overriding the control and log DB names in the configuration database,
    # and also using $ENV{SDF_DB_PREFIX} to generate the target DB names
    
    # TODO: consolidate these options ...
    
    my $db_prefix_from_config = $globals->SIMPLE_SELECT( 'SDF_DB_PREFIX' );
    
    if ( $db_prefix_from_config ) {
        
        $globals->CONTROL_DB_NAME( $db_prefix_from_config . "_CONTROL" );
        $globals->LOG_DB_NAME( $db_prefix_from_config . "_LOG" );
        
    } elsif ( $ENV{SDF_DB_PREFIX} ) {
        
        $globals->CONTROL_DB_NAME( $ENV{SDF_DB_PREFIX} . '_CONTROL' );
        $globals->LOG_DB_NAME( $ENV{SDF_DB_PREFIX} . '_LOG' );
        
    }
    
    # Now merge in the command-line options ...
    $globals->PARAMETER_RECURSION_LIMIT( $P_PARAMETER_RECURSION_LIMIT );
    $globals->MAX_CONCURRENT_PROCESSES( $P_MAX_CONCURRENT );
    $globals->MAX_ERRORS( $P_MAX_ERRORS );
    $globals->EXTRACT_DATE( $P_EXTRACT_DATE );
    $globals->FIFO_BASE( $p_fifo_base );
    $globals->COMMAND_LINE_ARGS( $args );
    
    # We also support a 'misc' hash in the global variables, where you can chuck key/value pairs.
    # This should just be used for once-off things where it's hard to justify registering 'proper' spaces & accessors for them.
    # DO NOT ABUSE!
    
    my $misc = {};
    
    $globals->MISC( $misc );
    
    # Now execute the function that corresponds to our run type ...
    
    my $worker;
    
    if ( $JOB_ID ) {
        
        $globals->JOB(
            SmartAssociates::Database::Item::Job::Base::generate(
                $globals
              , $JOB_ID
            )
        );
        
        $globals->JOB->field(
            &SmartAssociates::Database::Item::Job::Base::FLD_LOG_PATH
          , $log->log_path
        );
        
        $globals->JOB->update;
        $globals->JOB->start;

        $worker = SmartAssociates::ProcessingGroup::Base::generate(
            $globals
        );
        
        $globals->CONFIG_GROUP( $worker );
            
    } elsif ( $p_processing_group ) {
        
        $worker = SmartAssociates::WorkCollection::Base->new(
            $globals
          , $p_processing_group
          , $p_simulate
        );
        
    } elsif ( $p_extract_group ) {
        
        $worker = SmartAssociates::WorkCollection::Extraction->new(
            $globals
          , $p_extract_group
          , $p_simulate
        );
        
    } elsif ( $p_batch_id ) {
        
        $worker = SmartAssociates::WorkCollection::Base->new(
            $globals
          , $p_processing_group
          , $p_simulate
          , $p_batch_id
        );
        
    } elsif ( $p_work_collection ) {
        
        $worker = SmartAssociates::Base::generate(
            $globals
          , 'SmartAssociates::WorkCollection::' . $p_work_collection
          , $args
        );
        
    }
    
    $worker->prepare();
    $worker->execute();
    $worker->complete();
    
    my $batch = $globals->BATCH();
    my $job   = $globals->JOB();
    
    if ( $batch ) {
        $log->info( "Batch [" . $batch->key_value . "] exiting" );
    } elsif ( $job ) {
        $log->info( "Job [" . $job->key_value . "] exiting" );
    } else {
        $log->warn( "Some process that isn't a batch or job exiting" );
    }
    
};

my $err = $@;

if ( $err ) {
    $log->fatal( $err );
}

exit( SmartAssociates::Base::EXIT_CODE_SUCCESSFUL );
