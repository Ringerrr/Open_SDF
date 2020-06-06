package SmartAssociates::Database::Connection::Redshift;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Postgres';

use constant DB_TYPE            => 'Redshift';

sub build_connection_string {

    my ( $self, $auth_hash ) = @_;

    no warnings 'uninitialized';

    my $string =
          "dbi:ODBC:"
        . "DRIVER="               . $auth_hash->{ODBC_driver}
#        . ";DbUser="              . $auth_hash->{Username}
#        . ";Password="            . $auth_hash->{Password}
        . ";Database="            . $auth_hash->{Database}
        . ";Server="              . $auth_hash->{Host}
        . ";Port="                . $auth_hash->{Port};

    print "Redshift.pm assembled connection string: $string\n";

    return $self->SUPER::build_connection_string( $auth_hash, $string );

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

    my $sth = $self->prepare( "select pg_last_query_id() as pg_last_query_id" );

    $self->execute( $sth );

    my $results = $sth->fetchrow_hashref();

    return {
        query_id    => $results->{pg_last_query_id}
    };

}

1;
