package SmartAssociates::Database::Connection::Redshift;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Postgres';

use constant DB_TYPE            => 'Redshift';

sub build_connection_string {

    my ( $self, $auth_hash , $connection_string ) = @_;

    no warnings 'uninitialized';

    $connection_string =
        "dbi:ODBC:"
        . "DRIVER="               . $auth_hash->{ODBC_driver}
#        . ";DbUser="              . $auth_hash->{Username}
#        . ";Password="            . $auth_hash->{Password}
        . ";Database="            . $auth_hash->{Database}
        . ";Server="              . $auth_hash->{Host}
        . ";Port="                . $auth_hash->{Port}
        . ";SSLMode="             . $auth_hash->{Attribute_1};

    return $self->SUPER::build_connection_string( $auth_hash, $connection_string );

}

sub connect_post {

    my ( $self , $auth_hash , $options_hash ) = @_;
    
    my $dbh = $self->dbh;
    
    $dbh->{LongReadLen} = 65535 * 1024; # 64MB
    $dbh->{LongTruncOk} = 1;
    $dbh->{odbc_ignore_named_placeholders} = 1;

    return;

}

sub capture_execution_info {

    my ( $self , $sth ) = @_;
    
    my $this_sth = $self->prepare( "select pg_last_query_id() as pg_last_query_id" );
    
    $self->execute( $this_sth );
    
    my $results = $this_sth->fetchrow_hashref();
    
    return {
        query_id    => $results->{pg_last_query_id}
    };
    
}

sub error_strings_to_downgrade {

    my $self = shift;

    my @error_strings = (
        "INFO" # eg INFO:  Load into table 'coldroomtemperatures' completed, 4 record(s) loaded successfully.
    );

    return \@error_strings;

}

1;
