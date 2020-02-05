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

sub default_port {

    my $self = shift;

    return 1521;

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

sub fetch_column_info {

    my ( $self , $database , $schema , $table ) = @_;

    $table =~ s/"//g; # Strip out quotes - we quote reserved words in get_fields_from_table()

    my $cached_field_metadata = $self->field_metadata_cache;

    if ( ! exists $cached_field_metadata->{ $database }->{ $schema }->{ $table } ) {

        my $sth = $self->prepare(
            "select\n"
          . "    COLUMN_NAME\n"
          . "  , DATA_TYPE\n"
          . "  , case when DATA_TYPE like '%CHAR%' then '(' || CHAR_LENGTH || ')'\n"
          . "         when DATA_TYPE = 'NUMBER' then '(' || coalesce( DATA_PRECISION, 38 ) || ',' || DATA_SCALE || ')'\n"
          . "         else ''\n"
          . "    end as PRECISION\n"
          . "  , case when NULLABLE = 'Y' then 1 else 0 end as NULLABLE\n"
          . "  , DATA_DEFAULT        as COLUMN_DEFAULT\n"
          . "from\n"
          . "    all_tab_columns\n"
          . "where\n"
          . "    OWNER = ? and TABLE_NAME = ?\n"
          . "order by\n"
          . "    COLUMN_ID" )
            || $self->log->fatal( "Failed to fetch column info for [" . $self->db_schema_table_string( $database , $schema , $table ) . "]!" . $self->dbh->errstr );

        $self->execute( $sth, [ $schema , $table ] )
            || $self->log->fatal( "Failed to fetch column info for [" . $self->db_schema_table_string( $database , $schema , $table ) . "]!" . $self->dbh->errstr );

        my $field_metadata = $sth->fetchall_hashref( "COLUMN_NAME" );

        $sth->finish();

        $cached_field_metadata->{ $database }->{ $schema }->{ $table } = $field_metadata;

        $self->field_metadata_cache( $cached_field_metadata );

    }

    return $cached_field_metadata->{ $database }->{ $schema }->{ $table };

}

sub fetch_column_type_info {

    my ( $self, $database, $schema, $table, $column , $usage ) = @_;

    # This part blows. Oracle doesn't make clear distinctions between things like DATE, DATETIME, and TIMESTAMP column types.
    # As a result, when we fetch the column type codes in this method, we always get a TIMESTAMP ( 93 - SQL_TYPE_TIMESTAMP ).
    # This in turn frigs our formatted_select logic, as Oracle doesn't let us use a timestamp format string on a date column.

    my $column_type_info = $self->SUPER::fetch_column_type_info( $database , $schema , $table , $column , $usage );

    if ( $column_type_info->{column_info}->{DATA_TYPE} eq 'DATE' ) {
        $self->log->warn( "Oracle hack: Swapping type code for [$column] to the DATE constant ..." );
        $column_type_info->{type_code}        = &SmartAssociates::Database::Connection::Base::SQL_TYPE_DATE;
        $column_type_info->{formatted_select} = $self->formatted_select( $database , $schema , $table , $column , $column_type_info->{type_code} , $usage )
    }

    return $column_type_info;

}

1;
