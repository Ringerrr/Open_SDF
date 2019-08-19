package SmartAssociates::Database::Item::Batch::Postgres;

use strict;
use warnings;

use base 'SmartAssociates::Database::Item::Batch::Base';

sub fetchNextId {
    
    my $self = shift;
    
    my $sth = $self->dbh->prepare( "select nextval('BATCH_CTL_SEQ')" );
    
    $self->dbh->execute( $sth );
    
    my @row = $sth->fetchrow_array(
    ) || $self->log->fatal( $sth->errstr );
    
    $sth->finish();
    
    return $row[0]; # $row[0] is the 1st record in @row
    
}

sub update {
    
    my $self = shift;
    
    # Note: we don't ( currently ) support updating other fields such as APPL_NAME etc.
    # This should never really change
    
    my $sql = "update batch_ctl set\n"
            . "    END_TS          = now()::timestamp\n"
            . "  , STATUS          = ?\n"
            . "  , PROCESSING_TIME = extract ( epoch from now()::timestamp ) - extract( epoch from start_ts::timestamp )\n"
            . "where\n"
            . "    BATCH_ID        = ?";
    
    my $sth = $self->dbh->prepare( $sql );
    
    $self->dbh->execute(
        $sth
      , [
            $self->record->{ &SmartAssociates::Database::Item::Batch::Base::FLD_STATUS }
          , $self->key_value
        ]
    );
    
    $sth->finish();
    
}

1;
