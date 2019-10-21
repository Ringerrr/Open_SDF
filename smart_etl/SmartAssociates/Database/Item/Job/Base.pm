package SmartAssociates::Database::Item::Job::Base;

use strict;
use warnings;

use JSON;

use File::Path;                                        # Contains rmtree

use base 'SmartAssociates::Database::Item::Base';
 
my $IDX_LOG_PARTS_STH                               =  SmartAssociates::Database::Item::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_LOAD_EXECUTION_STH                          =  SmartAssociates::Database::Item::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_LOAD_EXECUTION_SEQ_STH                      =  SmartAssociates::Database::Item::Base::FIRST_SUBCLASS_INDEX + 2;

use constant    FIRST_SUBCLASS_INDEX                => SmartAssociates::Database::Item::Base::FIRST_SUBCLASS_INDEX + 3;

# These constants define statuses that we stamp in job control tables
use constant    STATUS_READY                        => 'READY';                     # Job is defined by master, but not yet started
use constant    STATUS_RUNNING                      => 'RUNNING';                   # Job is currently running
use constant    STATUS_COMPLETE                     => 'COMPLETE';                  # Job has completed successfully
use constant    STATUS_ERROR                        => 'ERROR';                     # Job has ended in an error state
use constant    STATUS_UNHANDLED_ERROR              => 'UNHANDLED_ERROR';           # Job has encountered an unhandled error and ended
use constant    STATUS_KILLED                       => 'KILLED';                    # Job was killed from above ( eg by a processing group set it a sibling process died )

# These constants are for each field in JOB_CTL
use constant    FLD_BATCH_ID                        => 'BATCH_ID';
use constant    FLD_PROCESSING_GROUP_NAME           => 'PROCESSING_GROUP_NAME';
use constant    FLD_IDENTIFIER                      => 'IDENTIFIER';
use constant    FLD_JOB_ARGS                        => 'JOB_ARGS';
use constant    FLD_EXTRACT_TS                      => 'EXTRACT_TS';
use constant    FLD_START_TS                        => 'START_TS';
use constant    FLD_END_TS                          => 'END_TS';
use constant    FLD_STATUS                          => 'STATUS';
use constant    FLD_PROCESSING_TIME                 => 'PROCESSING_TIME';
use constant    FLD_HOSTNAME                        => 'HOSTNAME';
use constant    FLD_ERROR_MESSAGE                   => 'ERROR_MESSAGE';
use constant    FLD_LOG_PATH                        => 'LOG_PATH';

# These constants are for items we can pack into FLD_JOB_ARGS
use constant    ARG_TEMP_FILES                      => 'TMP_FILES';

# Note that START_TS, END_TS and PROCESSING_TIME are NOT included above. These should use DB values ...

use constant    MSG_PART_TYPE_LOG                   => 'log';
use constant    MSG_PART_TYPE_ERROR                 => 'error';

# Constants for job args
use constant    JOB_ARG_EXTRACTION_SEQUENCE         => 'EXTRACTION_SEQUENCE';

sub generate {
    
    # This STATIC FUNCTION ( not a method ) will determine which subclass of SmartAssociates::Database::Item::Job
    # we need, and construct an object of that type
    
    my $globals             = $_[0];
    my $identifier          = $_[1];
    my $constructor_args    = $_[2];
    
    my $connection_name     = 'METADATA';
    my $connection_class    = $globals->CONNECTION_NAME_TO_DB_TYPE( $connection_name );
    
    my $job_class           = 'SmartAssociates::Database::Item::Job::' . $connection_class;

    my $job_object          = SmartAssociates::Base::generate(
                                  $globals
                                , $job_class
                                , $identifier
                                , $connection_name
                                , $constructor_args
                              );

    return $job_object;
    
}

sub connect {
    
    my $self            = shift;
    
    $self->dbh(
        SmartAssociates::Database::Connection::Base::generate(
            $self->globals
          , 'METADATA'                      # Connection name: the special name for our metadata connection
          , $self->globals->LOG_DB_NAME
        )
    );
    
}

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1] );
    
    # We can be instantiated from BOTH the batch ( parent ) process AND job ( child ) process
    # If we're a batch, then we're going to be inserting, and we won't be passed a key_field.
    # If we're a job, then we're going to be passed a key_field, and we pull that record into memory.
    
    $self->key_value(      $_[2] );
    my $connection_name  = $_[3];
    my $constructor_args = $_[4];
    
    $self->connect( $connection_name );
    
    $self->database_table( 'JOB_CTL' );
    $self->key_field( 'JOB_ID' );
    
    $self->prepare();
    
    if ( $constructor_args ) {
        $self->insert(
            $constructor_args
        );
    } else {
        $self->query();
    }
    
    return $self;
    
}

sub prepare {
    
    # The prepare method should create statement handles:
    #  - log_parts_sth
    #  - load_execution_sth
    # See the Postgres subclass for a complete implementation
    
    my $self = shift;
    
    $self->log->fatal( "Subclasses must implement the prepare() method" );
    
}

sub log_execution {
    
    my ( $self , $processing_group_name , $sequence_order , $tgt_db_nme
       , $tgt_tbl_nme , $start_ts , $end_ts , $record_count , $error , $template_sql
       , $perf_stats , $notes , $custom_logs ) = @_;
    
    $self->log->fatal( "Subclasses must implement the log_execution() method" );
    
}

sub part_to_log {
    
    my ( $self, $load_execution_id, $part_type, $text ) = @_;
    
    my $log_length = length( $text );
    
    my $position = 0;
    my $counter  = 0;
    
    while ( $position <= $log_length ) {
        
        $counter ++;
        
        my $part = substr( $text, $position, $self->MSG_PART_SIZE );
        
        $self->dbh->execute(
            $self->log_parts_sth
          , [
                $load_execution_id
              , $part_type
              , $counter
              , $part
            ]
        );
        
        $position += $self->MSG_PART_SIZE;
        
    }
    
}

sub fetchNextId {
    
    # This method can be implemented by subclasses that don't have auto-incrementing primary keys.
    # Some subclasses ( eg Postgres & Oracle ) that support sequences can use them. Others
    # have sequences, but don't guarantee they're returned in order, should have some
    # fancier logic for generating an ordered sequence.
    
    # The default ( return undef ) will get passed into the 'insert' statement, and DBs that *do*
    # have auto-incrementing PKs should return us an ID in the insert method.
    
    my $self = shift;
    
    return undef;
    
}

sub insert {
    
    # This method *should* be fine for all subclasses. It uses 'where not exists' logic, which may
    # need slight tweaking.
    
    my ( $self, $constructor_args ) = @_;
    
    $self->key_value( $self->fetchNextId() );
    
    # We try to prevent duplicate job definitions running simultaneously. This can either be
    # on the basis of *just* the PROCESSING_GROUP_NAME, *or* a combination of
    # PROCESSING_GROUP_NAME *and* IDENTIFIER. For example, in the case of loading from flat files,
    # we can support multiple concurrent loads, and we use the source filename as the IDENTIFIER.
    
    my ( $duplicate_lock_part, $identifier );
    
    if ( exists $constructor_args->{IDENTIFIER} && $constructor_args->{IDENTIFIER} ) {
        $duplicate_lock_part = "    and IDENTIFIER = ?\n";
        $identifier = $constructor_args->{IDENTIFIER};
    } else {
        $duplicate_lock_part = "    and IDENTIFIER is null\n";
    }
    
    my $sql =  "insert into job_ctl\n"
             . "(\n"
             . "    JOB_ID\n"
             . "  , BATCH_ID\n"
             . "  , PROCESSING_GROUP_NAME\n"
             . "  , IDENTIFIER\n"
             . "  , EXTRACT_TS\n"
             . "  , START_TS\n"
             . "  , END_TS\n"
             . "  , STATUS\n"
             . "  , PROCESSING_TIME\n"
             . "  , JOB_ARGS\n"
             . "  , HOSTNAME\n"
             . ") select\n"
             . "    ?\n"
             . "  , ?\n"
             . "  , ?\n"
             . "  , ?\n"
             . "  , ?\n"
             . "  , NULL\n"
             . "  , NULL\n"
             . "  , ?\n"
             . "  , NULL\n"
             . "  , ?\n"
             . "  , ?\n"
#             . "from _v_dual\n"
             . "where not exists\n"
             . "(\n"
             . "    select\n"
             . "        JOB_ID\n"
             . "    from\n"
             . "        JOB_CTL\n"
             . "    where\n"
             . "        PROCESSING_GROUP_NAME = ?\n"
             . "    and "
             . "        STATUS in\n"
             . "        (\n"
             . "            '" . SmartAssociates::Database::Item::Job::Base::STATUS_READY   . "'\n"
             . "          , '" . SmartAssociates::Database::Item::Job::Base::STATUS_RUNNING . "'\n"
             . "        )\n"
             . $duplicate_lock_part
             . ")";
    
    my $sth = $self->dbh->prepare( $sql );
    
    my $job_args = defined $constructor_args->{JOB_ARGS} ? JSON::encode_json( $constructor_args->{JOB_ARGS} ) : undef;

    my $batch_obj = $self->globals->BATCH;
    if ( ! $batch_obj ) {
        $self->globals->BATCH(
            SmartAssociates::Database::Item::Batch::Base::generate(
                $self->globals
              , undef
              , undef
              , $self->globals->JOB->field( FLD_BATCH_ID )
            )
        );
    }

    my $bind_values = [
        $self->key_value
      , $self->globals->BATCH->key_value
      , $constructor_args->{PROCESSING_GROUP}->processing_group_name
      , $constructor_args->{IDENTIFIER}
      , $constructor_args->{EXTRACT_TS}
      , SmartAssociates::Database::Item::Job::Base::STATUS_READY
      , $job_args
      , $self->hostname()
      , $constructor_args->{PROCESSING_GROUP}->processing_group_name
    ];
    
    if ( $identifier ) {
        push @{$bind_values}, $identifier;
    }
    
    my $inserted_no = $self->dbh->execute(
        $sth
      , $bind_values
    );
    
    $sth->finish();
    
    if ( $inserted_no eq &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED ) {
        
        # The above insert will affect 0 records if
        # the file_name we're trying to insert already exists in job_ctl.
        # This could be the case if we're invoked multiple times on the
        # same directory in fast succession
        
        no warnings 'uninitialized';
        
        $self->log->info( "Detected an existing job for the current processing group ["
            . $constructor_args->{PROCESSING_GROUP}->processing_group_name . "] ... IDENTIFIER [" . $identifier . "]" );
        
        $self->key_value( &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED ); # Caller should check our key_value for this and decide what to do
        
        return;
        
    }
    
    $self->log->info( "Job Created:\n"
       . "    Job ID:           [" . $self->key_value . "]\n"
       . "    Processing Group: [" . $constructor_args->{PROCESSING_GROUP}->processing_group_name . "]\n"
    );
    
    $self->query();
    
}

sub start {
    
    my $self = shift;
    
    # This method just sets the START_TS. We don't set it on inserting, because a batch process can
    # create a job but then wait a long period of time if it hits its max concurrent job limit.
    # We get called in etl.pl ...
    
    my $sql = "update JOB_CTL set\n"
            . "    START_TS        = now()\n"
            . "  , STATUS          = ?\n"
            . "where\n"
            . "    JOB_ID          = ?\n";
    
    my $sth = $self->dbh->prepare( $sql );
    
    my $rec = $self->record;
    
    $self->dbh->execute(
        $sth
      , [
            STATUS_RUNNING
          , $self->key_value
        ]
    );
    
    $sth->finish();
    
    my $log_dir = $self->job_log_dir();
    
    $self->log->info( "Creating temporary log directory [$log_dir] ..." );
    
    mkdir( $log_dir )
        || $self->log->warn( $! );
    
}

sub job_log_dir {
    
    my $self = shift;
    
    return $self->globals->LOGDIR . "/" . $self->key_value;
    
}

sub update {
    
    my $self = shift;
    
    $self->log->fatal( "Subclasses must implement the update() method" );
    
}

sub begin {
    
    my $self = shift;
    
    $self->record->{ &FLD_STATUS } = STATUS_RUNNING;
    $self->update;
    
}

sub job_arg {
    
    my ( $self, $arg, $value ) = @_;
    
    my $rec = $self->record;
    my $encoded_job_args = $rec->{ &FLD_JOB_ARGS };
    my $args_hash = {};
    
    if ( defined $encoded_job_args ) {
        $args_hash = decode_json( $encoded_job_args );
    }
    
    if ( defined $value ) {
        $args_hash->{ $arg } = $value;
        $encoded_job_args = encode_json( $args_hash );
        $rec->{ &FLD_JOB_ARGS } = $encoded_job_args;
        $self->update;
    }
    
    return $args_hash->{ $arg };
    
}

sub get_temporary_files {
    
    my $self = shift;
    
    my $temporary_files = $self->job_arg( &ARG_TEMP_FILES );
    
    my $return = ();
    
    if ( $temporary_files ) {
        $return = decode_json( $temporary_files );
    }
    
    return $return;
    
}

sub add_temporary_file {
    
    my ( $self, $tmp_file ) = @_;
    
    my $temporary_files = $self->job_arg( &ARG_TEMP_FILES );
    
    my $files = ();
    
    if ( $temporary_files ) {
        $files = decode_json( $temporary_files );
    }
    
    push @{$files}, $tmp_file;
    
}

sub complete {
    
    my $self = shift;
    
    $self->record->{ &FLD_STATUS } = STATUS_COMPLETE;
    
    $self->update;
    
    my $log_dir = $self->job_log_dir();
    
    $self->log->info( "Removing temporary log directory [$log_dir] ..." );
    
    rmtree( $log_dir )
        || $self->log->fatal( $! );
    
}

sub log_parts_sth           { return $_[0]->accessor( $IDX_LOG_PARTS_STH,             $_[1] ); }
sub load_execution_sth      { return $_[0]->accessor( $IDX_LOAD_EXECUTION_STH,        $_[1] ); }
sub load_execution_seq_sth  { return $_[0]->accessor( $IDX_LOAD_EXECUTION_SEQ_STH,    $_[1] ); }

1;
