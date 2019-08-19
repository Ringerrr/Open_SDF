package SmartAssociates::Database::Connection::Firebird;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Base';

use constant DB_TYPE            => 'Firebird';

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    my $connection_string =
          "dbi:Firebird:"
        . "db="        .$auth_hash->{Database} . ";"
        . "Host="     . $credentials->{Host};
    
    return $self->SUPER::build_connection_string( $auth_hash, $connection_string );
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table , $options ) = @_;
    
    if ( ! $options->{dont_quote} ) {
        return $table;
    } else {
        return '"' . $table . '"';
    }
    
}

1;
