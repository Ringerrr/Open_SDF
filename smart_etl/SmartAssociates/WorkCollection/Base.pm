package SmartAssociates::WorkCollection::Base;

use strict;
use warnings;

use JSON;

use base 'SmartAssociates::Base';

my $IDX_CONTROL_HASH                            =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_PROCESSING_GROUP_NAME                   =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_SIMULATE                                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_DBH                                     =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 3;
my $IDX_BATCH_ID                                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 4;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 5;

# This class has a 1:1 mapping with CONTROL..PROCESSING_GROUP
# We basically check to see whether the group is disabled or not ...
# then launch it in a child process

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1] );
     
    $self->[ $IDX_PROCESSING_GROUP_NAME ]       = $_[2];
    $self->[ $IDX_SIMULATE ]                    = $_[3];
    $self->[ $IDX_BATCH_ID ]                    = $_[4];
    
    if ( ! $self->[ $IDX_PROCESSING_GROUP_NAME ] && ! $self->[ $IDX_BATCH_ID ] ) {
        $self->log->fatal( "SmartAssociates::WorkCollection::Base needs either a processing group name, or a batch id" );
    }
    
    # NOTE: if you add more args to the constructor, you MUST alter subclasses accordingly ...
    
    $self->[ $IDX_DBH ] = SmartAssociates::Database::Connection::Base::generate(
        $self->globals
      , 'METADATA'
      , $self->globals->CONTROL_DB_NAME
    );
    
    return $self;
    
}

sub prepare {
    
    my $self = shift;
    
    if ( $self->[ $IDX_BATCH_ID ] ) {
        
        $self->globals->BATCH(
            SmartAssociates::Database::Item::Batch::Base::generate(
                $self->globals
              , undef
              , undef
              , $self->[ $IDX_BATCH_ID ]
            )
        );
        
        $self->[ $IDX_PROCESSING_GROUP_NAME ] = $self->globals->BATCH->field( &SmartAssociates::Database::Item::Batch::Base::FLD_BATCH_IDENTIFIER );
        
    } else {
        
        $self->globals->BATCH(
            SmartAssociates::Database::Item::Batch::Base::generate(
                $self->globals
              , $self->processing_group_name
            )
        );
    }
    
    $self->getControlHash();
    
}

sub getControlHash {
    
    my $self = shift;
    
    my $sql =
        "select\n"
      . "    PROCESSING_GROUP.PROCESSING_GROUP_NAME             as PROCESSING_GROUP_NAME\n"
      . "  , PROCESSING_GROUP.DISABLE_FLAG                      as DISABLE_FLAG\n"
      . "  , count(CONFIG.SEQUENCE_ORDER)                       as TEMPLATE_STEPS\n"
      . "from\n"
      . "            CONFIG\n"
      . "inner join  PROCESSING_GROUP\n"
      . "    on\n"
      . "            CONFIG.PROCESSING_GROUP_NAME = PROCESSING_GROUP.PROCESSING_GROUP_NAME\n"
      . "where\n"
      . "    PROCESSING_GROUP.PROCESSING_GROUP_NAME = ?\n";
    
    my $bind_values = [];
    push @{$bind_values}, $self->[ $IDX_PROCESSING_GROUP_NAME ];
    
    $sql .= "group by\n"
      . "    PROCESSING_GROUP.PROCESSING_GROUP_NAME\n"
      . "   ,PROCESSING_GROUP.DISABLE_FLAG";
    
    my $sth = $self->[ $IDX_DBH ]->prepare(
        $sql
    );
    
    $self->[ $IDX_DBH ]->execute(
        $sth
      , $bind_values
    );
    
    $self->control_hash( $sth->fetchall_hashref( "PROCESSING_GROUP_NAME" ) );
    
    $sth->finish();
    
}

sub execute {
    
    my $self = shift;
    
    my $batch = $self->globals->BATCH;
    
    $batch->field(
        SmartAssociates::Database::Item::Batch::Base::FLD_STATUS
      , SmartAssociates::Database::Item::Batch::Base::STATUS_RUNNING
    );
    
    $batch->update;
    
    $self->log->info( "Processing group [" . $self->[ $IDX_PROCESSING_GROUP_NAME ] . "] starting up ..." );
    
    my $control_hash = $self->[ $IDX_CONTROL_HASH ];
    
    # Loop through all items in the process group
    foreach my $key ( keys %{$control_hash} ) {
        
        $self->log->info( "<" . ( '=' x 20 ) . "> Next Item: [" . $key . "]\n" );
        
        my $identifier;
        
        # Check if this item has any templates registered
        if ( ! $control_hash->{ $key }->{TEMPLATE_STEPS} ) {
            
            $self->log->info( "Processing Group [$key] doesn't have any registered templates. Skipping ..." );
            
            next; # This will jump back up to the next item in the 'foreach' loop
            
        }
        
        # Check if disable_flg is set for this config
        if ( $control_hash->{ $key }->{DISABLE_FLAG} ) {
            
            $self->log->info( "Processing Group [$key] has its DISABLE_FLG flag set. Skipping ..." );
            
            next; # This will jump back up to the next item in the 'foreach' loop
            
        }
        
        my $job_args = {};
        
        # Merge batch args into job args
        my $batch = $self->globals->BATCH;
        
        my $batch_args_string = $batch->field( &SmartAssociates::Database::Item::Batch::Base::FLD_BATCH_ARGS );
        
        if ( defined $batch_args_string ) {
            $job_args = decode_json( $batch_args_string );
        }
        
        if ( $job_args->{ '#P_MIGRATION_CONTROL_ID#' } ) {
            
            no warnings "uninitialized";
            $identifier = $key . "_-_" . $job_args->{ '#P_MIGRATION_CONTROL_ID#' };
            
        }
        
        my $extract_date      = $self->globals->EXTRACT_DATE;
        
        if ( $self->simulate ) {
            $job_args->{simulate} = 1;
        }
        
        my $job = SmartAssociates::Database::Item::Job::Base::generate(
            $self->globals
          , undef    # job id
          , {
                PROCESSING_GROUP    => $self
              , EXTRACT_TS          => $extract_date
              , JOB_ARGS            => $job_args
              , IDENTIFIER          => $identifier
            }
        );
        
        if ( $job->key_value eq &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED ) {
            $self->log->fatal( "Failed to insert job. There is probably a job in the READY or RUNNING state already" );
        }
        
        $self->startChildProcess(
            SmartAssociates::Base::CHILD_TYPE_JOB
          , $job->key_value
        );
        
    }
    
}

sub complete {
    
    my $self = shift;
    
    my $active_processes = $self->globals->ACTIVE_PROCESSES();
    
    if ( $active_processes ) {
        
        $self->log->info( ref $self . "->complete() called ... waiting for [ $active_processes ] children to complete ..." );
        
        while ( $active_processes ) {
            $self->captureExitingChild();
            $active_processes --;
        }
        
    }
    
    $self->log->info( "All children complete ..." );
    
    my $batch_item = $self->globals->BATCH;
    $batch_item->complete();
    
    # requery, to bring in some stats, which we save back to the processing_group record
    # this makes it much easier to display in the main processing groups screen
    
    $batch_item->query();
    
    # Check for failures in the batch. If there are failures, we *don't* want to update the processing_group
    # record, because it's handy to use the PG screen as a dashboard to see what's been run and when ( and
    # it makes more sense to only show jobs that completed successfully ).
    
    if ( $batch_item->field( &SmartAssociates::Database::Item::Batch::Base::FLD_STATUS )
      eq &SmartAssociates::Database::Item::Batch::Base::STATUS_COMPLETE
    ) {
        $self->[ $IDX_DBH ]->do(
            "update processing_group set\n"
          . "    last_run_timestamp = '" . $batch_item->field( 'END_TS' ) . "'\n"
          . "  , last_run_seconds = " . $batch_item->field( 'PROCESSING_TIME' ) . "\n"
          . "where\n"
          . "    processing_group_name = '" . $self->[ $IDX_PROCESSING_GROUP_NAME ] . "'"
        );
    }
    
}

sub control_hash                { return $_[0]->accessor( $IDX_CONTROL_HASH,                    $_[1] ); }
sub simulate                    { return $_[0]->accessor( $IDX_SIMULATE,                        $_[1] ); }
sub processing_group_name       { return $_[0]->accessor( $IDX_PROCESSING_GROUP_NAME,           $_[1] ); }
sub dbh                         { return $_[0]->accessor( $IDX_DBH,                             $_[1] ); }

1;
