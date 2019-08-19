package SmartAssociates::Database::Item::Batch::Base;

use strict;
use warnings;

use base 'SmartAssociates::Database::Item::Base';
 
my $IDX_IDENTIFIER                                  =  SmartAssociates::Database::Item::Base::FIRST_SUBCLASS_INDEX + 0;

use constant    FIRST_SUBCLASS_INDEX                => SmartAssociates::Database::Item::Base::FIRST_SUBCLASS_INDEX + 1;

# These constants define statuses that we stamp in job control tables
use constant    STATUS_READY                        => 'READY';                     # Batch ready to run
use constant    STATUS_RUNNING                      => 'RUNNING';                   # Batch currently executing
use constant    STATUS_COMPLETE                     => 'COMPLETE';                  # Batch completed with all child jobs successful
use constant    STATUS_ERROR                        => 'COMPLETE_WITH_ERROR';       # Batch completed with some child jobs in error state
use constant    STATUS_UNHANDLED_ERROR              => 'UNHANDLED_ERROR';           # Batch has encountered an unhandled error and ended
use constant    STATUS_KILLED                       => 'KILLED';                    # Batch was killed from above ( eg by a processing group set it a sibling process died )

# These constants are for each field in BATCH_CTL ( that we expose )
use constant    FLD_START_TS                        => 'START_TS';
use constant    FLD_END_TS                          => 'END_TS';
use constant    FLD_STATUS                          => 'STATUS';
use constant    FLD_PROCESSING_TIME                 => 'PROCESSING_TIME';
use constant    FLD_HOSTNAME                        => 'HOSTNAME';
use constant    FLD_BATCH_IDENTIFIER                => 'BATCH_IDENTIFIER';
use constant    FLD_BATCH_ARGS                      => 'BATCH_ARGS';

sub generate {
    
    # This STATIC FUNCTION ( not a method ) will determine which subclass of SmartAssociates::Database::Item::Batch
    # we need, and construct an object of that type
    
    my $globals             = $_[0];
    my $identifier          = $_[1];
    my $constructor_args    = $_[2];
    my $batch_id            = $_[3];
    
    my $connection_name     = 'METADATA';
    my $connection_class    = $globals->CONNECTION_NAME_TO_DB_TYPE( $connection_name );
    
    my $batch_class         = 'SmartAssociates::Database::Item::Batch::' . $connection_class;

    my $batch_object        = SmartAssociates::Base::generate(
                                  $globals
                                , $batch_class
                                , $identifier
                                , $connection_name
                                , $constructor_args
                                , $batch_id
                              );

    return $batch_object;
    
}

sub connect {
    
    my $self = shift;
    
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
    
    $self->[ $IDX_IDENTIFIER ]  = $_[2];
    
    my $connection_name         = $_[3]      || $self->log->fatal( "Missing arg [CONNECTION NAME]" );
    
    my $constructor_args        = $_[4];
    
    my $batch_id                = $_[5];
    
    if ( ! $self->[ $IDX_IDENTIFIER ] && ! $batch_id ) {
        $self->log->fatal(
            "SmartAssociates::Database::Item::Batch::Base needs either an IDENTIFIER ( processing group name ) or a BATCH_ID"
        );
    }
    
    $self->connect( $connection_name );
    
    $self->database_table( 'BATCH_CTL' );
    $self->key_field( 'BATCH_ID' );
    
    if ( $batch_id ) {
        $self->key_value( $batch_id );
        $self->query();
    } else {
        $self->insert(
            $constructor_args
        );
    }
    
    return $self;
    
}

sub fetchNextId {
    
    # This method can be implemented by subclasses that don't have auto-incrementing primary keys.
    # Some subclasses ( eg Postgres & Oracle ) that support sequences can use them.
    
    # The default ( return undef ) will get passed into the 'insert' statement, and DBs that *do*
    # have auto-incrementing PKs should return us an ID in the insert method.
    
    my $self = shift;
    
    return undef;
    
}

sub insert {
    
    my ( $self, $constructor_args ) = @_;
    
    $self->key_value( $self->fetchNextId() );
    
    my $sql =  "insert into batch_ctl\n"
             . "(\n"
             . "    BATCH_ID\n"
             . "  , BATCH_IDENTIFIER\n"
             . "  , START_TS\n"
             . "  , END_TS\n"
             . "  , STATUS\n"
             . "  , PROCESSING_TIME\n"
             . "  , HOSTNAME\n"
             . "  , BATCH_ARGS\n"
             . ") values (\n"
             . "    ?\n"
             . "  , ?\n"
             . "  , now()\n"
             . "  , NULL\n"
             . "  , '" . STATUS_RUNNING . "'\n"
             . "  , NULL"
             . "  , ?"
             . "  , ?"
             . ")";
    
    my $sth = $self->dbh->prepare( $sql );
    
    my $batch_args_string = defined $constructor_args->{BATCH_ARGS} ? JSON::encode_json( $constructor_args->{BATCH_ARGS} ) : undef;
    
    $self->dbh->execute(
        $sth
      , [
            $self->key_value
          , $self->[ $IDX_IDENTIFIER ]
          , $self->hostname()
          , $batch_args_string
       ]
    );
    
    $self->query();
    
}

sub update {
    
    my $self = shift;
    
    # Note: we don't ( currently ) support updating other fields such as APPL_NAME etc.
    # This should never really change
    
    $self->log->fatal( "Subclasses must implement the update() method" );
    
}

sub complete {
    
    my $self = shift;
    
    # Now we have to decide what final status to save in batch_ctl, and what
    # return code to provide to Espresso
    my $status_check_sth = $self->dbh->prepare(
          "select\n"
         . "    count(JOB_ID) as ERROR_NUMBER\n"
         . "from\n"
         . "    JOB_CTL\n"
         . "where\n"
         . "    BATCH_ID = ?\n"
         . "and STATUS  != '" . SmartAssociates::Database::Item::Batch::Base::STATUS_COMPLETE . "'"
    );
    
    $self->dbh->execute(
        $status_check_sth
      , [ $self->key_value ]
    );
    
    my $status_check = $status_check_sth->fetchrow_hashref()
        || $self->log->warn( "Failed to check status of jobs in batch!\n" . $status_check_sth->errstr );
    
    my ( $batch_completion_status, $exit_code );
    
    if ( $status_check->{ERROR_NUMBER} > 0 ) {
        
        $batch_completion_status    = STATUS_ERROR;
        
    } else {
        
        $batch_completion_status    = STATUS_COMPLETE;
        
    }
    
    $status_check_sth->finish();
    
    $self->record->{ &FLD_STATUS } = $batch_completion_status;
    
    $self->update;
    
}

1;
