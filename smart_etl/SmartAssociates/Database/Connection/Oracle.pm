package SmartAssociates::Database::Connection::Oracle;

use strict;
use warnings;

use Exporter qw ' import ';

use base 'SmartAssociates::Database::Connection::Base';

use constant DB_TYPE            => 'Oracle';

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1], $_[2], $_[3], $_[4] );
    
    $self->dbh->do( "alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss'" )
        || $self->log->fatal( "Couldn't set a sane nls_date_format!" . $self->dbh->errstr );

    $self->dbh->do( "alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss'" )
        || $self->log->fatal( "Couldn't set a sane nls_timestamp_format!" . $self->dbh->errstr );

    return $self;
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    # Oracle is strange. We never rebuild the connection string, as a username is the same
    # thing as a database.
    
    return $self->SUPER::build_connection_string( $auth_hash, $auth_hash->{ConnectionString} );
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table , $options ) = @_;
    
    if ( ! $options->{dont_quote} ) {
        return $schema . '.' . $table;
    } else {
        return '"' . $schema . '"."' . $table . '"';
    }
    
}

sub coalesce {
    
    my ( $self , $expression , $string ) = @_;
    
    # Oracle doesn't allow us to swap data types in a coalesce, so we have to
    # convert to CHAR explicitly ...
    
    return "coalesce(to_char($expression),$string)";
    
}

sub does_table_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select OBJECT_NAME from ALL_OBJECTS where OBJECT_TYPE = 'TABLE' and OWNER = '" . $schema . "' and OBJECT_NAME = '" . $table . "'";

    return $sql;

}

sub does_schema_not_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select 'not exists' from dual where not exists ( select USERNAME from ALL_USERS where USERNAME = '" . $schema . "' )";

    return $sql;

}

sub create_expression_md5sum {

    my $self    = shift;
    my $options = shift;

    my $all_columns = $self->concatenate( $options );

    my $expression = "'0x' || standard_hash( $all_columns , 'MD5' ) as HASH_VALUE";

    return $expression;

}

1;
