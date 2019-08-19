package window::data_loader::Postgres;

use warnings;
use strict;

use parent 'window::data_loader::Connection';

use Glib qw( TRUE FALSE );

use feature 'switch';

sub column_mapper {
    
    # This method maps columns in window::data_loader to DB-specific columns
    
    my ( $self, $def ) = @_;
    
    my $type;
    
    # For columns that didn't have any data, assume a varchar(100)
    if ( ! $def->{type} ) {
        $def->{type} = "VARCHAR";
        $def->{max_length} = 100;
    }
    
    # Map DATETIME to TIMESTAMP for Postgres
    if ( $def->{type} eq &window::data_loader::DATETIME ) {
        $def->{type} = 'TIMESTAMP';
    }
    
    if ( $def->{type} =~ /CHAR/i ) {
        
        # The max_length is only the max we've seen so far, and I'm getting sick of re-reading files
        # with bigger & bigger number of rows read each time, just to get the max length. Let's just
        # go for 100 by default, and make it bigger if we've detected it already.
        
        my $calc_length;
        
        if ( $def->{max_length} > 100 ) {
            $calc_length = $def->{max_length};
        } elsif ( $def->{max_length} < 5 ) {
            $calc_length = $def->{max_length};
        } else {
            $calc_length = 100;
        }
        
        $type = $def->{type} . "($calc_length)";
        
    } elsif ( $def->{type} ne &window::data_loader::DATE && $def->{type} ne &window::data_loader::TIMESTAMP && $def->{type} ne &window::data_loader::INT ) {
        
        $type = $def->{type} . "(" . $def->{scale} . ")";
        
    } elsif ( $def->{type} eq &window::data_loader::INT ) {
        
        given ( $def->{scale} ) {
            when ( $_ == &window::data_loader::SCALE_BYTEINT )  { $type = 'SMALLINT' }       # Postgres doesn't have a BYTEINT type
            when ( $_ == &window::data_loader::SCALE_TINYINT )  { $type = 'SMALLINT' }
            when ( $_ == &window::data_loader::SCALE_SMALLINT ) { $type = 'SMALLINT' }
            when ( $_ == &window::data_loader::SCALE_INT )      { $type = 'INT' }
            when ( $_ == &window::data_loader::SCALE_BIGINT )   { $type = 'BIGINT' }
        }
        
    } else {
        
        $type = $def->{type};
        
    }
    
    #return "VARCHAR";
    
    return $type;
    
}

1;