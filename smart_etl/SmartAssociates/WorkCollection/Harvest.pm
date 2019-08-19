package SmartAssociates::WorkCollection::Harvest;

use strict;
use warnings;

use File::Copy;                                         # Libraries to copy, move, rename files
use File::Find::Rule;                                   # Search files recursively
use File::Path qw ' make_path rmtree ';                 # Contains rmtree & mk_path ( recursive mkdir )
use IO::Uncompress::Gunzip qw / gunzip $GunzipError /;  # Libraries to uncompress gzip files

use Data::Dumper;

use base 'SmartAssociates::WorkCollection::Base';

my $IDX_TOPLEVEL_DIR                            =  SmartAssociates::WorkCollection::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_PID_TO_FILE_INFO                        =  SmartAssociates::WorkCollection::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_DISABLE_DECOMPRESSION                   =  SmartAssociates::WorkCollection::Base::FIRST_SUBCLASS_INDEX + 2;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::WorkCollection::Base::FIRST_SUBCLASS_INDEX + 3;

# The Harvst class is the batch process that controls loading from flat files.
# It has logic for file handling
# ( moving files between /incoming /processing /complete /error directories as we complete various stages )
# and can include business logic ( eg protection from loading files in an incorrect order ).

# Harvest mode should be called in a GROUPING_CODE, which defines which files we'll
# process in the current run. The idea is that if we've got thousands of files from
# a couple of high-level groups, we can load these in parallel by launching multiple
# harvest runs with different grouping codes. This is simpler to schedule in the case
# where we are sensitive to loading files in the correct order, eg:

# - type ABC - order-sensitive ... 105 files on day 1, 200 files on day 2
# - type XYZ - order-sensitive ... 2 files on day 2, 1000 files on day 2

# In this example, we don't want type ABC holding up XYZ. While we could schedule
# everything in this case, it would be much more complicated. Further, if the business
# want to prioritise everything from type XYZ, then they can set up their scheduling
# to process all of XYZ 1st, and only then start processing ABC.

sub new {
    
    #my $self = $_[0]->SUPER::new( $_[1], $_[2] );
    
    my ( $class, $globals, $args ) = @_;
    
    #if ( ! $args->{harvest_group} ) {
    #    $self->log->fatal( "Missing JSON arg: harvest_group" );
    #}
    
    my $self = $class->SUPER::new( $globals, $args->{harvest_group} );
    
    if ( $args->{harvest_root_dir} ) {
        $ENV{HARVEST_ROOT_DIR} = $args->{harvest_root_dir};
    } elsif ( ! $ENV{HARVEST_ROOT_DIR} ) {
        $self->log->fatal( "Harvest mode requires either a JSON arg: harvest_root_dir, or the environment variable HARVEST_ROOT_DIR to be set" );
    }
    
    if ( $args->{disable_decompression} ) {
        $self->[ $IDX_DISABLE_DECOMPRESSION ] = 1;
    }
    
    $self->[ $IDX_TOPLEVEL_DIR ]  = $ENV{HARVEST_ROOT_DIR};
    
    $self->[ $IDX_PID_TO_FILE_INFO ] = {};
    
    return $self;
    
}

sub prepare {
    
    my $self = shift;
    
    $self->globals->BATCH(
        SmartAssociates::Database::Item::Batch::Base::generate(
            $self->globals
          , $self->processing_group_name
        )
    );
    
    $self->getControlHash();
    $self->setupPaths();
    
}

sub getControlHash {
    
    my $self = shift;
    
    # When we're running in harvest mode, the batch process loads details of all harvest groups
    # ( ie PROCESSING_GROUP records ) of a particular GROUPING_CODE ( this is in the mappings table )
    
    my $sth = $self->dbh->prepare(
        "select\n"
      . "    HC.*\n"
      . "from\n"
      . "           PROCESSING_GROUP        PG\n"
      . "inner join HARVEST_CONTROL         HC\n"
      . "               on PG.PROCESSING_GROUP_NAME = HC.PROCESSING_GROUP_NAME\n"
      . "where\n"
      #. "    upper(PG.TAGS) like '%HARVEST%'\n"
      . "    PG.DISABLE_FLAG = 0\n"
      . "and HC.GROUPING_CODE = ?"
    );
    
    $self->dbh->execute( $sth, [ $self->processing_group_name ] );
    
    $self->control_hash( $sth->fetchall_hashref( "FILE_REGEX" ) );
    
    $sth->finish();
    
}

sub setupPaths {
    
    my $self = shift;
    
    my $dir_separator = $self->globals->DIR_SEPARATOR;
    
    foreach my $folder ( "incoming", "processing", "complete", "error" ) {
        my $this_dir = $self->[ $IDX_TOPLEVEL_DIR ] . $dir_separator . $folder;
        if ( ! -d $this_dir ) {
            $self->log->warn( $this_dir . " doesn't exist. Creating it ..." );
            eval { make_path( $this_dir ) };
            if ( $@ ) {
                $self->log->fatal( "Harvest directory [$this_dir] doesn't exist, and attempt to create it failed:\n" . $@ );
            }
        }
    }
    
}

sub execute {
    
    my $self = shift;
    
    $self->log->info( "Harvesting directory: [" . $self->[ $IDX_TOPLEVEL_DIR ] . "]" );
    
    my $directory_separator = $self->globals->DIR_SEPARATOR;
    
    my $incoming_dir = $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "incoming/";
    
    my @fileList;
    
    push @fileList, File::Find::Rule->file()
                                    ->name( "*" )
                                    ->in( $incoming_dir );
    
    my $files_by_date;
    
    my $control_hash = $self->control_hash;
    
    # Loop through all files in the directory
    foreach my $this_file_path ( @fileList ) {
        
        my @path_components = split( $directory_separator, $this_file_path );
        
        my $file = $path_components[$#path_components];
        
        # NOTE: Any expected failure should fail the file, not die()
        
        $self->log->info( "\n<" . ( '=' x 50 ) . "> Next File: [" . $file . "]\n" );
        
        # Now we loop through all our mappings, and look for a regex pattern that matches
        # our filename. When we find one, we use it to parse out the components and assign
        # them to items in a $file_details hash. The KEYS of this hash are dynamially generated,
        # based on the POS_x values in the mappings table. Using this mappings table and dynamic
        # key allocation, we can bring more file types online by altering metadata, and ( usually )
        # no code changes are required.
        
        my $harvest_control = $self->control_hash;
        my $file_info;
        
        foreach my $file_regex ( keys %{$harvest_control} ) {
            
            $self->log->debug( "Testing current file with regex: [   $file_regex   ]" );
            
            my @matches = $file =~ /$file_regex/i;
            
            if ( @matches ) {
                
                # $control is a hash, based on the HARVEST_CONTROL table
                my $control = $harvest_control->{$file_regex};
                
                # Now check if a RECORD_LENGTH has been defined. If so, we need to peek inside the file to see
                # if it's the correct file format.
                if ( $control->{RECORD_LENGTH} ) {
                    
                    open RECORD_LENGTH_TEST, "<$this_file_path"
                        || $self->log->fatal( "Failed to open [$this_file_path]!\n" . $! );
                    
                    my $record = <RECORD_LENGTH_TEST>;
                    
                    close RECORD_LENGTH_TEST;
                    
                    chomp( $record ); # get rid of new-lines
                    
                    my $length = length( $record );
                    
                    if ( $length != $control->{RECORD_LENGTH} ) {
                        
                        $self->log->debug( "File [$this_file_path] is length [$length] but the current control record wants ["
                            . $control->{RECORD_LENGTH} . "]. Skipping to next control record ..." );
                        
                        next;
                        
                    } else {
                        
                        $self->log->debug( "File [$this_file_path] has length [$length], which matches the current control record."
                            . " Selecting ..." );
                        
                    }
                    
                }
                
                for my $match_counter ( 1 .. 15 ) {
                    my $match_key = $control->{ 'POS_' . $match_counter }; 
                    if ( $match_key ) {
                        $file_info->{$match_key} = $matches[ $match_counter - 1 ]; # @matches starts at 0
                    }
                }
                
                $file_info->{PERFORM_LOAD_ORDER_CHECK}  = $control->{PERFORM_LOAD_ORDER_CHECK};
                $file_info->{PATH}                      = $this_file_path;
                $file_info->{FILE}                      = $file;
                $file_info->{PROCESSING_GROUP_NAME}     = $control->{PROCESSING_GROUP_NAME};
                
                if ( -l $file_info->{PATH} ) {
                    $file_info->{SYMBOLIC_LINK} = 1;
                }
                
                last; # Stop looking for further patterns that match
                
            }
            
        }
        
        if ( ! $file_info ) {
            $self->log->debug( "No regex matches for current file ... skipping to next file" );
            next;
        }
        
        # For convenience / readability, we pluck out some common values from the $file_info hash
        my ( $extract_date );
        
        if ( exists $file_info->{EXTRACT_DATE} ) {
            
            $extract_date = $file_info->{EXTRACT_DATE};
        
            # Parse and re-assemble the date using '-' separators. This allows us to do
            # a simple string-sort 
            
            my ( $this_yyyy, $this_mm, $this_dd, $this_hour, $this_min, $this_sec );
            
            if ( $extract_date =~ /(\d{4})[-\/]*(\d{2})[-\/]*(\d{2}).*(\d{2})[:]*(\d{2})[:]*(\d{2})/ ) {
                
                $this_yyyy = $1;
                $this_mm   = $2;
                $this_dd   = $3;
                $this_hour = $4;
                $this_min  = $5;
                $this_sec  = $6;
                
            } elsif ( $extract_date =~ /(\d{4})[-\/]*(\d{2})[-\/]*(\d{2})/ ) { # TODO: merge into above
                
                $this_yyyy = $1;
                $this_mm   = $2;
                $this_dd   = $3;
                
            } else {
                
                if ( $file_info->{PERFORM_LOAD_ORDER_CHECK} ) {
                    $self->log->fatal( "HARVEST_CONTROL specifies an EXTRACT_DATE,"
                                     . " but parsing the date [$extract_date] failed. Defaulting to NOW!" );
                } else {
                    $self->log->warn( "No extract date was parsed from the file. Defaulting to NOW!" );
                    $extract_date = $self->log->timestamp;
                }
                
                $extract_date = $self->log->timestamp;
                
            }
            
            $extract_date = $this_yyyy . "-" . $this_mm . "-" . $this_dd;
            
            if ( $this_hour ) {
                $extract_date .= ' ' . $this_hour . ':' . $this_min . ':' . $this_sec;
            }
            
        } else {
            
            if ( $file_info->{PERFORM_LOAD_ORDER_CHECK} ) {
                $self->log->fatal( "HARVEST_CONTROL says we need to perform a load order check, but no extract date was located" );
            } else {
                $self->log->warn( "No extract date was parsed from the file. Defaulting to NOW!" );
                $extract_date = $self->log->timestamp;
            }
            
        }
        
        # Then assemble a hash of dates, with file info inside.
        # We want to complete processing all files for a given date before we progress to the next.
        # We key the inner hash on filename, so we can do a sort on this too. This way if there are sequence numbers
        # inside the filename, we automatically sort of them also, without having to code even more sorting logic.
        
        $files_by_date->{ $extract_date }->{ $file } = $file_info;
        
    }
    
    foreach my $date ( sort( keys %{$files_by_date} ) ) {
        
        $self->log->info( "Launching jobs for date [$date]" );
        
        my $load_order_checked = 0;
        
        foreach my $file ( sort ( keys %{$files_by_date->{ $date }} ) ) {
            
            my $file_info = $files_by_date->{ $date }->{ $file };
            
            if ( $file_info->{PERFORM_LOAD_CHECK_ORDER} ) {
                $load_order_checked = 1; # This will make us wait for all children to complete before rolling to the next day
                $self->checkIngestionLoadOrder( $file_info ); # this will $self->log->fatal() if any errors are encountered
            }   # TODO: RENAME above
            
            # Unzip if necessary
            # TODO: support more compression types
            
            if ( $file_info->{FILE} =~ /(.*)\.gz/ && ! $self->[ $IDX_DISABLE_DECOMPRESSION ] ) {
                
                my $unzipped_file = $1;
                
                my $pigz_path = `which pigz`;
                chomp( $pigz_path );
                
                if ( -f $pigz_path ) {
                    
                    $self->log->info( "Parallel unzipping ( via pigz ) file ..." );
                    
                    my @args = (
                        $pigz_path
                      , "-f"
                      , "-k"
                      , "-d"
                      , $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "incoming"   . $directory_separator . $file
                    );
                    
                    system( @args ) == 0
                        or $self->log->fatal( "pigz unzip failed: " . $? );
                    
                    # We need to move this file to the 'processing' directory
                    move(
                        $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "incoming"   . $directory_separator . $unzipped_file
                      , $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "processing"
                    ) || $self->log->fatal( "Failed to move [" . $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "incoming"   . $directory_separator . $unzipped_file . "] to ["
                                          . $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "processing" . "]\n" . $! );
                    
                } else {
                    
                    $self->log->info( "Unzipping file ..." );
                    
                    my $status = gunzip $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "incoming"   . $directory_separator . $file
                                     => $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "processing" . $directory_separator . $unzipped_file
                        || $self->log->fatal( "gunzip failed: " . $! );
                    
                }
                
                $self->log->info( "  ... unzipping complete" );
                
                $file_info->{UNZIPPED_FILE} = $unzipped_file;
                
            }
            
            # At this point, we move the object from /incoming into /processing
            
            move(
                $file_info->{PATH}
              , $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "processing"
            ) || $self->log->fatal( "Failed to move [" . $file_info->{PATH} . "] to ["
                                  . $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "processing" . "]\n" . $! );
            
            my $job_args = {
                harvest => 1
            };
            
            if ( $self->simulate ) {
                $job_args->{simulate} = 1;
            }
            
            # For Harvest jobs, we swap our processing group name in here, as we're called with a GROUP code,
            # and not a processing group name
            $self->processing_group_name( $file_info->{PROCESSING_GROUP_NAME} );
            
            my $job = SmartAssociates::Database::Item::Job::Base::generate(
                $self->globals
              , undef
              , {
                    PROCESSING_GROUP    => $self
                  , IDENTIFIER          => $file_info->{UNZIPPED_FILE} ? $file_info->{UNZIPPED_FILE} : $file_info->{FILE}
                  , EXTRACT_TS          => $date
                  , JOB_ARGS            => $job_args
                }
            );
            
            if ( $job->key_value eq &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED ) {
                $self->log->fatal( "Failed to insert job. There is probably a job in the READY or RUNNING state already" );
            }
            
            # Create a directory for the .nzlog and .nzbad files
            my $dir = $self->log->log_dir . $self->globals->DIR_SEPARATOR . $job->key_value;
            
            mkdir( $dir )
                || $self->log->fatal( "Failed to create nzlog/nzbad directory: [$dir]:\n" . $! );
            
            my $pid = $self->startChildProcess(
                SmartAssociates::Base::CHILD_TYPE_JOB
              , $job->key_value
            );
            
            $self->[ $IDX_PID_TO_FILE_INFO ]->{$pid} = $file_info;
            
        }
        
        if ( $load_order_checked ) {
            
            my $active_processes = $self->globals->ACTIVE_PROCESSES;
            
            while ( $active_processes ) {
                $self->captureExitingChild();
                $active_processes --;
            }
            
            $self->globals->ACTIVE_PROCESSES( $active_processes );
            
        }
        
    }
    
}

sub checkHarvestOrder {
    
    my ( $self, $file_info ) = @_;
    
    # This function checks whether it's safe to load the file, with info we've been passed.
    # TODO: rewrite queries below and TEST
    
    $self->log->info( "checkHarvestOrder() checking whether it's safe to run for file:\n" . Dumper( $file_info ) );
    
    my $sth = $self->dbh->prepare(
        "select\n"
      . "    EXTRACT_TS\n"
      . "  , cast( EXTRACT_TS + interval '1 day' as date ) as NEXT_DATE"
      . "  , JOB_ID\n"
      . "from\n"
      . "    LOG..job_ctl\n"
      . "where job_id = ( select max( job_id ) from LOG..job_ctl job_ctl_inner where process_grp_id = 1 and type = ? and STATUS = 'COMPLETE' )"
    );
    
    $self->dbh->execute(
        $sth
      , [ $file_info->{type} ]
    );
    
    my $last_successful_load_info = $sth->fetchrow_hashref;
    
    if ( ! $last_successful_load_info ) {
        
        $self->log->warn( "checkIngestionLoadOrder didn't find ANY previous ingestion runs for type [" . $file_info->{type} . "]"
               . " in JOB_CTL. Maybe this is the 1st run for this file type?" );
        
        $sth->finish();
        
        return;
        
    } else {
        
        $sth->finish();
        
    }
    
    # Beware: the below string comparison expects our extract_ts ( on both sides ) to be in PRECISELY the format: YYYY-MM-DD HH:MM:SS
    if ( $last_successful_load_info->{EXTRACT_TS} gt $file_info->{extract_date} . " " . $file_info->{extract_time} ) {
        $self->log->fatal( "Detected a COMPLETE job with a date [" . $last_successful_load_info->{EXTRACT_TS} . "]"
                . " beyond the current file [" . $file_info->{extract_date} . " " . $file_info->{extract_time} . "]. This is a FATAL error" );
    }
    
    if ( $last_successful_load_info->{NEXT_DATE} ne $file_info->{extract_date} ) {
        
        # We're trying to process a file that's > 1 day beyond the last successful run date
        # While we don't implement all the checks in IDW ( see above note ), we do need to check whether there are any
        # failed jobs for the dates in between the last successful run and now
        
        my $sth = $self->dbh->prepare(
            "select\n"
          . "    *\n"
          . "from\n"
          . "    LOG..JOB_CTL\n"
          . "where\n"
          . "        process_grp_id = 1\n"          # TODO: this query needs rewriting
          . "    and type = ?\n"
          . "    and STATUS != 'COMPLETE'\n"
          . "    and extract_ts > ?\n"
          . "    and extract_ts < ?\n"
          . "order by\n"
          . "    extract_ts"
        );
        
        $self->dbh->execute(
            $sth
          , [
                $file_info->{type}
              , $last_successful_load_info->{EXTRACT_TS}
              , $file_info->{extract_date} . " " . $file_info->{extract_time}
            ]
        );
        
        my $error_run = $sth->fetchrow_hashref;
        
        $sth->finish();
        
        if ( $error_run ) {
            
            $self->log->fatal( "We've calculated based on the last successful extraction date [" . $last_successful_load_info->{EXTRACT_TS} . "]"
                             . " that the NEXT extraction date should be [" . $last_successful_load_info->{NEXT_DATE} . "]"
                             . " but we have encountered extract date [" . $file_info->{extract_date} . " " . $file_info->{extract_time} . "]."
                             . " Further, there is an INCOMPLETE run dated [" . $error_run->{EXTRACT_TS} . "] beyond the last successful"
                             . " run date, but this is NOT the date we're currently trying to load."
                             . " This is a FATAL error." );
            
        } else {
            
            $self->log->warn( "Detected a gap between the last sucecssful run [" . $last_successful_load_info->{EXTRACT_TS} . "]"
                            . " and this run [" . $file_info->{extract_date} . " " . $file_info->{extract_time} . "]."
                            . " However, no unsuccessful runs exist in between these dates, so assuming it's safe to continue ..."
            );
            
        }
        
    }
    
}

sub handleChildSuccess {
    
    my ( $self, $pid , $job ) = @_;
    
    $self->SUPER::handleChildSuccess( $pid , $job );
    
    # Jobs can complete 'successfully' in terms of an exit signal, yet still be in an 'error'
    # state. We have to query the JOB_CTL table to see how things actually went ...
    
    my $status = $job->field( SmartAssociates::Database::Item::Job::Base::FLD_STATUS );
    
    if ( $status ne SmartAssociates::Database::Item::Job::Base::STATUS_COMPLETE ) {
        
        $self->handleChildError( $pid );
        
    } else {
        
        my $file_info = $self->[ $IDX_PID_TO_FILE_INFO ]->{$pid};
        
        my $directory_separator = $self->globals->DIR_SEPARATOR;
        
        move( $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . 'processing' . $directory_separator . $file_info->{FILE}
            , $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "complete" )
                || $self->log->warn( "Failed to move [" . $file_info->{FILE} . "] to ["
                    . $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . "complete" . "]\n" . $! );
        
        if ( $file_info->{UNZIPPED_FILE} && ! $self->[ $IDX_DISABLE_DECOMPRESSION ] ) {
            unlink $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . 'processing' . $directory_separator . $file_info->{UNZIPPED_FILE};
        }
        
        my $tmp_files = $job->get_temporary_files;
        
        foreach my $tmp_file ( @{$tmp_files} ) {
            $self->log->debug( "Deleting temporary file: [$tmp_file]" );
            unlink $tmp_file;
        }
        
        # Delete the job-specific nzlog/nzbad directory ( we're not interested in these if the job succeeded )
#        my $dir = $self->log->log_dir . $self->globals->DIR_SEPARATOR . $job->key_value;
#        rmtree( $dir );
        
    }
    
}

sub handleChildError {
    
    my ( $self, $pid , $job ) = @_;
    
    $self->SUPER::handleChildError( $pid , $job ); # pass "don't die"
    
    my $file_info = $self->[ $IDX_PID_TO_FILE_INFO ]->{$pid};
    
    my $directory_separator = $self->globals->DIR_SEPARATOR;
    
    move( $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . 'processing' . $directory_separator . $file_info->{FILE}
        , $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . 'error' )
            || $self->log->fatal( "Failed to move ["
                . $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . 'processing' . $directory_separator . $file_info->{FILE} . "] to ["
                . $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . 'error' . "]\n" . $! );
    
    if ( $file_info->{UNZIPPED_FILE} && ! $self->[ $IDX_DISABLE_DECOMPRESSION ] ) {
        unlink $self->[ $IDX_TOPLEVEL_DIR ] . $directory_separator . 'processing' . $directory_separator . $file_info->{UNZIPPED_FILE};
    }
    
    # TODO: If a Harvest job ended in an error, there's a pretty high chance that it left an external table
    #       in the target database. We really should handle this. We'd have to query the CONFIG table to
    #       find the target database & target table, detokenize the table part, and search in the catalog
    #       for the table ... deleting it if we found it.
    
}

sub toplevel_dir                       { return $_[0]->accessor( $IDX_TOPLEVEL_DIR,                   $_[1] ); }

1;
