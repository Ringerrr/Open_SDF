package SmartAssociates::Database::Connection::SQLite;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Base';

use constant    DB_TYPE     => 'SQLite';

sub new {

    my $self = $_[0]->SUPER::new( $_[1], $_[2], $_[3], $_[4] );

    $self->dbh->do( "PRAGMA default_synchronous = OFF" );

    return $self;

}

sub build_connection_string {

    my ( $self, $auth_hash ) = @_;

    # TODO: flag to flip file / mem based
    return "dbi:SQLite:dbname=" . $auth_hash->{Host};
    
}

1;