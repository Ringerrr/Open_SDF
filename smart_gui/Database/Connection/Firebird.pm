package Database::Connection::Firebird;

use parent 'Database::Connection';

use strict;
use warnings;

use feature 'switch';

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'hex';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'Firebird';

use Glib qw | TRUE FALSE |;

sub default_port {

    my $self = shift;

    return -1;

}

sub connection_label_map {
    
    my $self = shift;
    
    return {
        Username        => "Username"
      , Password        => "Password"
      , Database        => ""
      , Host_IP         => "Host / IP"
      , Port            => "Port"
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
    };
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    no warnings 'uninitialized';
    
    if ( ! $auth_hash->{Driver} ) {
        $auth_hash->{Driver} = 'Firebird';
    }
    
    my $string =
          "dbi:Firebird:"
        . ";Host="                . $auth_hash->{Host};
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub connect_post {
    
    my ( $self, $auth_hash, $options_hash ) = @_;
    
    if ( $auth_hash->{Database} ) {
        $self->{connection}->do( "use " . $auth_hash->{Database} );
    }
    
}

sub fetch_database_list {
    
    my $self = shift;

    return ( "dummy" );

}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;

    my @tables = $self->tables;

    foreach my $table ( @tables ) {
        $table =~ s/"//g; # strip quotes
    }

    return sort( @tables );
    
}

sub fetch_view_list {
    
    my ( $self, $database ) = @_;
    
    my @return;
    
    return sort( @return );
    
}

sub fetch_view {
    
    my ( $self, $database, $schema, $view_name ) = @_;
    
    return;
    
    my $sth = $self->prepare(
        "show create view " . $database . "." . $view_name
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my $row = $sth->fetchrow_arrayref;
    
    return $$row[1];
    
}

sub fetch_function_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return;
    
    my $sth = $self->prepare(
        "select FUNCTION from _v_function"
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_procedure_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return;
    
    my $sth = $self->prepare(
        "select name from mysql.proc where db = ?"
    ) || return;
    
    $self->execute( $sth, [ $database ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_procedure {
    
    my ( $self, $database, $schema, $procedure ) = @_;
    
    return;
    
    my $sth = $self->prepare(
        "select body_utf8 from mysql.proc where db = ? and name = ?"
    ) || return;
    
    $self->execute( $sth, [ $database, $procedure ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return $return[0];
    
}

sub fetch_column_info_array {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $return;

    my $sql = qq { SELECT\n
      r.RDB\$FIELD_NAME AS field_name
    , r.RDB\$DESCRIPTION AS field_description
    , r.RDB\$DEFAULT_VALUE AS field_default_value
    , r.RDB\$NULL_FLAG AS field_not_null_constraint
    , f.RDB\$FIELD_LENGTH AS field_length
    , f.RDB\$FIELD_PRECISION AS field_precision
    , f.RDB\$FIELD_SCALE AS field_scale
    , CASE f.RDB\$FIELD_TYPE
          WHEN 261 THEN 'BLOB'
          WHEN 14 THEN 'CHAR'
          WHEN 40 THEN 'CSTRING'
          WHEN 11 THEN 'D_FLOAT'
          WHEN 27 THEN 'DOUBLE'
          WHEN 10 THEN 'FLOAT'
          WHEN 16 THEN 'INT64'
          WHEN 8 THEN 'INTEGER'
          WHEN 9 THEN 'QUAD'
          WHEN 7 THEN 'SMALLINT'
          WHEN 12 THEN 'DATE'
          WHEN 13 THEN 'TIME'
          WHEN 35 THEN 'TIMESTAMP'
          WHEN 37 THEN 'VARCHAR'
          ELSE 'UNKNOWN'
      END AS field_type
    , f.RDB\$FIELD_SUB_TYPE AS field_subtype
    , coll.RDB\$COLLATION_NAME AS field_collation
    , cset.RDB\$CHARACTER_SET_NAME AS field_charset
    FROM             RDB\$RELATION_FIELDS r
    LEFT JOIN        RDB\$FIELDS f ON r.RDB\$FIELD_SOURCE = f.RDB\$FIELD_NAME
    LEFT JOIN        RDB\$COLLATIONS coll ON f.RDB\$COLLATION_ID = coll.RDB\$COLLATION_ID
    LEFT JOIN        RDB\$CHARACTER_SETS cset ON f.RDB\$CHARACTER_SET_ID = cset.RDB\$CHARACTER_SET_ID
    WHERE r.RDB\$RELATION_NAME=?
    ORDER BY r.RDB\$FIELD_POSITION };

    my $sth = $self->prepare( $sql )
        || return;

    $self->execute( $sth, [ $table ] )
        || return;

    while ( my $column_hash = $sth->fetchrow_hashref ) {

        $return->{ $column_hash->{FIELD_NAME} } = {
              COLUMN_NAME     => $column_hash->{FIELD_NAME}
            , DATA_TYPE       => $column_hash->{FIELD_TYPE}
            , PRECISION       => ( $column_hash->{FIELD_LENGTH} ? $column_hash->{FIELD_LENGTH} : $column_hash->{FIELD_PRECISION} )
            , NULLABLE        => ! $column_hash->{FIELD_NOT_NULL_CONSTRAINT}
        };
    }

    if ( $options->{force_upper} ) {
        $return = $self->mangle_case_result_data( $return, "upper" );
    }

    return undef;
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    my $column_info_array = $self->fetch_column_info_array( $database, $schema, $table, $options );
    
    my $return;
    
    foreach my $column_info ( @{$column_info_array} ) {
        
        $return->{ $column_info->{col_name} } = {
            COLUMN_NAME     => $column_info->{col_name}
          , DATA_TYPE       => $column_info->{data_type}
          , PRECISION       => undef
          , NULLABLE        => 1
        };
        
    }
    
    if ( $options->{force_upper} ) {
        $return = $self->mangle_case_result_data( $return, "upper" );
    }
    
    return $return;
    
}

sub fetch_field_list {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    return;

    my $column_info_array = $self->fetch_column_info_array( $database, $schema, $table, $options );
    
    my $fields;
    
    foreach my $column_info ( @{$column_info_array} ) {
        push @{$fields}, $column_info->{col_name};
    }
    
    return $fields;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    # $options contains:
    #{
    #    dont_quote      => 0
    #}

    # Hive doesn't appear to support cross-database queries ???

    if ( $options->{dont_quote} ) {
        return $table;
    } else {
        return '"' . $table . '"';
    }
}

sub has_schemas {
    
    my $self = shift;
    
    return 0;
    
}

1;
