package SmartAssociates::Base;

use strict;
use warnings;

use Data::Dumper;

my $IDX_GLOBALS                             =  0;

use constant    FIRST_SUBCLASS_INDEX        => 1;

use constant    EXIT_CODE_SUCCESSFUL        => 0;
use constant    EXIT_CODE_CHILD_ERROR       => 10;
use constant    EXIT_CODE_BATCH_ERROR       => 20;
use constant    EXIT_CODE_GROUP_SET_ERROR   => 30;

use constant    CHILD_TYPE_BATCH            => 'Batch';
use constant    CHILD_TYPE_JOB              => 'Job';

# This class is the base class of all other objects in the SmartAssociates framework.
# It holds global variables, accessor methods, and other handy stuff for
# other classes to inherit.

sub new {
    
    my ( $class, $globals ) = @_;
    
    my $self = [];
    
    bless $self, $class;
    
    $self->globals( $globals );
    
    return $self;
    
}

# Abstract data access method

sub accessor {
    
    my ( $self, $index_position, $data ) = @_;
    
    if ( defined $data ) {
        $self->[ $index_position ] = $data;
    }
    
    return $self->[ $index_position ];
    
}

sub comma_separated {
    
    my ( $self, $number ) = @_;
    
    $number =~ s/(\d)(?=(\d{3})+(\D|$))/$1\,/g;
    
    return $number;
    
}

sub hostname {
    
    my $self = shift;
    
    my ( $sysname, $nodename, $release, $version, $machine ) = POSIX::uname();
    
    return $nodename;
    
}

sub generate {
    
    my ( $globals, $object_class, @object_constructor_args ) = @_;
    
    # This STATIC FUNCTION ( not a method ) will attempt to locate
    # the perl file we need to 'include', and construct an object of that type
    
    # Convert path name into relative path
    my $class_relative_path = $object_class;
    $class_relative_path =~ s/:/\//g;
    
    # We also need the '.pm' at the end ...
    $class_relative_path .= '.pm';
    
    my @all_paths = $globals->ALL_ETL_PATHS;

    $globals->LOG->info( "Attempting to load class [$object_class]" );

    foreach my $include_path ( @all_paths ) {
        $globals->LOG->info( " Trying include path: [$include_path]" );
        if ( -e $include_path . "/" . $class_relative_path ) {
            $globals->LOG->info( "  Loading Perl resource [" . $include_path . "/" . $class_relative_path . "] for connection class [$object_class]" );
            eval {
                require $include_path . "/" . $class_relative_path;
            };
            my $error = $@;
            if ( $error ) {
                $globals->LOG->fatal( "Couldn't load class [$object_class]:\n$error" );
            }
            last;
        }
    }
    
    my $object = $object_class->new(
        $globals
      , @object_constructor_args
    );
    
    return $object;
    
}

sub startChildProcess {
    
    my ( $self, $process_type, $identifier, $options ) = @_;
    
    my $active_processes = $self->globals->ACTIVE_PROCESSES;
    my $max_concurrent   = $self->globals->MAX_CONCURRENT_PROCESSES;

    $self->log->info( "In startChildProcess() with [$active_processes] out of [$max_concurrent] active processes ..." );

    while ( $active_processes >= $max_concurrent ) {
        
        $self->log->info( "MAX_CONCURRENT process limit reached. Waiting for a child process to end ..." );
        
        $self->captureExitingChild();
        
        $active_processes --;
        
    }
    
    my $pid_to_job_id_mapping = $self->globals->PID_TO_JOB_ID_MAPPING();
    
    my $pid = fork;
    
    if ( $pid ) {
        
        # We're the master
        # Increment the number of active processes
        $active_processes ++;
        
        if ( $process_type eq CHILD_TYPE_JOB ) {
            
            # Also store this PID <==> Job ID info for later
            $pid_to_job_id_mapping->{ $pid } = $identifier;
            
            $self->globals->PID_TO_JOB_ID_MAPPING( $pid_to_job_id_mapping );
            
            $self->log->info( "The batch process just forked a child process with linux PID [$pid]" );
            
        } elsif ( $process_type eq CHILD_TYPE_BATCH ) {
            
            # Also store this PID <==> PG name info for later
            $pid_to_job_id_mapping->{ $pid } = $options->{batch_id};
            
            $self->globals->PID_TO_JOB_ID_MAPPING( $pid_to_job_id_mapping );
            
            $self->log->info( "The processing group set process just forked a child process with linux PID [$pid]" );
            
        }
        
        $self->globals->ACTIVE_PROCESSES( $active_processes );
        
        return $pid;
        
    } elsif ( defined ( $pid ) ) {
        
        # We're the child
        # We spawn an entirely new instance of this script, passing in the job_id to process
        
        use Cwd 'abs_path';                 # Get the full path of ourself in the next line
        my $self_name = abs_path( $0 );     # $0 is the name of ourself
        
        my @args;
        
        push @args
           , "--user-profile=" . $self->globals->USER_PROFILE
           , "--log-level="    . $self->log->log_level_text;
        
        if ( $process_type eq CHILD_TYPE_JOB ) {
            
            push @args,
              , "--job-id=$identifier";

            $self->log->info( "Launching job via ID: [$identifier]" );
          
        } elsif ( $process_type eq CHILD_TYPE_BATCH ) {
            
            $self->log->info( "Launching batch via ID: [" . $options->{batch_id} . "]" );
            
            push @args,
              , "--batch-id=" . $options->{batch_id};
            
        }

        $self->log->info( "Launching with args:\n" . Dumper( \@args ) );

        exec( 'perl'
            , "-I ."
            , $self_name
            , @args
        ) || $self->log->fatal( "exec() failed!\n" . $! );
        
    } else {
        
        $self->log->fatal( "Failed to fork!\n" . $! );
        
    }
    
}

sub captureExitingChildByPID {

    my ( $self , $pid ) = @_;

    my $returned_pid = waitpid( $pid , 0 )
        || $self->log->fatal( "waitpid failed!\n" . $! );;

    $self->handleExitingChild( $pid );

}

sub captureExitingChild {
    
    my $self = shift;
    
    # The wait function waits for a child process to end, and returns that process's PID as it ends

    my $pid = wait
        || $self->log->fatal( "wait failed!\n" . $! );

    $self->handleExitingChild( $pid );

}

sub handleExitingChild {

    my ( $self , $pid ) = @_;

    my $pid_to_job_id_mapping = $self->globals->PID_TO_JOB_ID_MAPPING();

    my $job = SmartAssociates::Database::Item::Job::Base::generate(
        $self->globals
      , $pid_to_job_id_mapping->{ $pid }
    );

    if ( $? ) {
        $self->handleChildError( $pid , $job );
    } else {
        $self->handleChildSuccess( $pid , $job );
    }

    my $log_path = $job->field( &SmartAssociates::Database::Item::Job::Base::FLD_LOG_PATH );

    eval {

        local $/;

        open( JOB_LOG_HANDLE , "<$log_path" )
            || die( "Failed to open job log file [$log_path]:\n" . $! );

        my $full_job_log = <JOB_LOG_HANDLE>;

        close JOB_LOG_HANDLE;

        my $sth = $job->dbh->prepare( "delete from job_full_log where job_id = ?" );

        $job->dbh->execute( $sth , [ $pid_to_job_id_mapping->{ $pid } ] );

        $sth = $job->dbh->prepare( "insert into job_full_log ( job_id , log_text ) values ( ? , ? )" );

        $job->dbh->execute( $sth , [ $pid_to_job_id_mapping->{ $pid } , $full_job_log ] );

    };

    my $err = $@;

    if ( $err ) {
        $self->log->warn( "Failed to update job_full_log:\n$err" );
    } else {
        unlink( $log_path )
          || $self->log->warn( "Failed to delete job log file:\n" . $! );
    }

    $self->log->info( "Process with PID [$pid] just completed ..." );
    
    return $pid;
    
}

sub handleChildError {
    
    my ( $self, $pid , $job ) = @_;

    # Note: we MUST have the leading ampersand here, to force these constants to resolve at run-time, otherwise
    # we have circular dependencies between SmartAssociates::Base and SmartAssociates::Database::Item::Job
    
    my $status = $job->field( &SmartAssociates::Database::Item::Job::Base::FLD_STATUS );
    
    # Only set the status to 'unhandled error' if it's currently 'complete'
    if ( $status eq  &SmartAssociates::Database::Item::Job::Base::STATUS_COMPLETE ) {
        $job->field( &SmartAssociates::Database::Item::Job::Base::FLD_STATUS, &SmartAssociates::Database::Item::Job::Base::STATUS_UNHANDLED_ERROR );
    }
    
    $job->update();
    
}

sub handleChildSuccess {
    
    my ( $self, $pid , $job ) = @_;
    
    # The default is to do nothing ...
    
    return;
    
}

sub globals                 { return $_[0]->accessor( $IDX_GLOBALS,                 $_[1] ); }

sub log                     { return $_[0]->globals->LOG;                                    } # convenience ...

1;
