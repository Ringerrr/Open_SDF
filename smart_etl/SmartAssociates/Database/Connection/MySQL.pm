package SmartAssociates::Database::Connection::MySQL;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Base';

use constant DB_TYPE            => 'MySQL';

sub default_port {
    
    my $self = shift;
    
    return 3306;
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    my $connection_string =
          "dbi:mysql:"
        . "database="  . ( $auth_hash->{Database} || 'test' )
        . ";host="     . $auth_hash->{Host}
        . ";port="     . $auth_hash->{Port}
        . ";mysql_use_result=1"; # prevent $dbh->execute() from pulling all results into memory
    
    return $self->SUPER::build_connection_string( $auth_hash, $connection_string );
    
}

sub connect_pre {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    $options_hash->{dbi_options_hash} = {
        RaiseError        => 0
      , AutoCommit        => 1
      , mysql_enable_utf8 => 1
    };
    
    $auth_hash->{ConnectionString} = undef;
    
    return ( $auth_hash , $options_hash );
        
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table , $options ) = @_;
    
    if ( ! $options->{dont_quote} ) {
        return $database . '.' . $table;
    } else {
        return '"' . $database . '"."' . $table . '"';
    }
    
}

sub does_table_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select TABLE_NAME from information_schema.TABLES where TABLE_TYPE like 'BASE TABLE' and TABLE_SCHEMA = '" . $schema . "' and TABLE_NAME = '" . $table . "'";

    return $sql;

}

1;
