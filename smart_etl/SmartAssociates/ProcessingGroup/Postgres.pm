package SmartAssociates::ProcessingGroup::Postgres;

use strict;
use warnings;

use base 'SmartAssociates::ProcessingGroup::Base';

my $IDX_LOG_DBH                                 =  SmartAssociates::ProcessingGroup::Base::FIRST_SUBCLASS_INDEX + 0;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::ProcessingGroup::Base::FIRST_SUBCLASS_INDEX + 1;

sub new {
    
    my $self   = $_[0]->SUPER::new( $_[1] );
    
    $self->[ $IDX_LOG_DBH ] = SmartAssociates::Database::Connection::Base::generate(
        $self->globals
      , 'METADATA'
      , $self->globals->LOG_DB_NAME
    );
    
    return $self;
    
}

sub getMetadata {
    
    my $self = shift;
    
    my $sth = $self->[ $IDX_LOG_DBH ]->prepare(
        "select\n"
      . "    PROCESSING_GROUP_NAME\n"
      . "from\n"
      . "            JOB_CTL\n"
      . "where\n"
      . "    JOB_CTL.JOB_ID = ?\n"
    );
    
    $self->[ $IDX_LOG_DBH ]->execute(
        $sth
      , [
            $self->globals->JOB->key_value
        ]
    );
    
    my $rec = $sth->fetchrow_hashref;
    
    $self->name( $rec->{PROCESSING_GROUP_NAME} );
    
    $sth->finish();
    
}

1;
