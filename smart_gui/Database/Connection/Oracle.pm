package Database::Connection::Oracle;

use parent 'Database::Connection';

use strict;
use warnings;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'rawtohex';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'Oracle';

use Data::Dumper;

sub new {
    
    my ( $class, $globals, $auth_hash, $dont_connect ) = @_;
    
    my $self = $class->SUPER::new( $globals, $auth_hash, $dont_connect );
    
    if ( ! $self ) {
        return undef;
    }
    
    # Remember some stuff for later for Oracle ...
    $self->{_database} = $auth_hash->{Database};
    $self->{_username} = $auth_hash->{Username};
    
    $self->{connection}->{LongReadLen} = 65535 * 1024; # 64MB
    
    return $self;
    
}

sub default_port {

    my $self = shift;

    return 1521;

}

sub connect_pre {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    if ( ! $auth_hash->{ConnectionString} ) {
        $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
    }
    
    return ( $auth_hash , $options_hash );
    
}

sub connect_post {
    
    my ( $self ) = @_;
    
    $self->do( "alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss'" );
    $self->do( "alter session set nls_timestamp_format='yyyy-mm-dd hh24:mi:ss'" );

    $self->{connection}->{LongReadLen} = 65535 * 1024; # 64MB
    
}

sub connection_label_map {

    my $self = shift;

    return {
        Username        => "Username"
      , Password        => "Password"
      , Database        => "SID"
      , Host_IP         => "Host"
      , Port            => "Port"
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };

}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    no warnings 'uninitialized';
    
    my $string =
          "dbi:Oracle:"
        . ( $auth_hash->{Database} || 'XE' )
        . ";host="      . $auth_hash->{Host}
        . ";Port="      . $auth_hash->{Port}
        . ";SID="       . ( $auth_hash->{Database} || 'XE' );
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub fetch_database_list {
    
    my $self = shift;
    
    # Oracle doesn't support multiple databases at the server level
    #  ... BUT we store the database in the connections table for Oracle,
    # so we just return this ( array with 1 element )
    
    return ( $self->{_database} );
    
}

sub fetch_schema_list {
    
    my ( $self, $database ) = @_;
    
    # my $sth = $self->prepare( "select USERNAME from ALL_USERS" )
    #     || return;
    #
    # $self->execute( $sth )
    #     || return;
    #
    # my @return;
    #
    # while ( my $row = $sth->fetchrow_arrayref ) {
    #     push @return, $$row[0];
    # }

    my @return = ( $self->{_username} );

    return sort( @return );
    
}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;
    
    my $sth = $self->prepare(
        "select OBJECT_NAME from ALL_OBJECTS where OBJECT_TYPE = 'TABLE' and OWNER = ?"
    ) || return;
    
    $self->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table ) = @_;
    
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
        || return;
    
    $self->execute( $sth, [ $schema , $table ] )
        || return;
    
    my $return = $sth->fetchall_hashref( "COLUMN_NAME" );
    
    return $return;
    
}

sub fetch_all_column_info {

    my ( $self, $database, $schema, $progress_bar, $options ) = @_; # $options currently unused

    # NOTE! We're fetching into an array, so if you change the SQL, you need to change the code below ...

    my $sth = $self->prepare(
        "select\n"
            . "    TABLE_NAME              as TABLE_NAME\n"
            . "  , COLUMN_NAME             as COLUMN_NAME\n"
            . "  , DATA_TYPE               as DATA_TYPE\n"
            . "  , case when DATA_TYPE like '%CHAR%' then '(' || CHAR_LENGTH || ')'\n"
            . "         when DATA_TYPE = 'NUMBER' then '(' || coalesce( DATA_PRECISION, 38 ) || ',' || case when DATA_SCALE is null then 0 else DATA_SCALE end || ')'\n"
            . "         else ''\n"
            . "    end as PRECISION\n"
            . "  , case when upper(NULLABLE) = 'Y' then 1\n"
            . "         else 0\n"
            . "    end                     as NULLABLE\n"
            . "  , DATA_DEFAULT            as COLUMN_DEFAULT\n"
            . "from\n"
            . "    all_tab_columns\n"
            . "where\n"
            . "    owner = ?\n"
            . "order by\n"
            . "    TABLE_NAME , COLUMN_ID"
    ) || return;

    if ( $progress_bar ) {
        $progress_bar->set_text( "Fetching column info from database ..." );
        $self->kick_gtk;
    }

    $self->execute( $sth, [ $schema ] )
        || return;

    # It's fastest to pull everything at once ...
    my $all_column_info = $sth->fetchall_arrayref;

    my $no_of_records = @{$all_column_info};

    my $counter;
    my $return;

    # The top-level of this structure is a hash, with table names as the key.
    # Inside that, we have an array of column info records, sorted in the order we get
    # from the DB ( so columns appear in the order they're normally seen in )
    foreach my $column_info ( @{$all_column_info} ) {
        if ( $progress_bar ) {
            $counter ++;
            if ( $counter % 100 == 0 ) {
                $progress_bar->set_fraction( $counter / $no_of_records );
                $progress_bar->set_text( $column_info->[1] . " ..." );
                $self->kick_gtk;
            }
        }
        push @{ $return->{ $column_info->[0] } }
            , {
                  COLUMN_NAME     => $column_info->[1]
                , DATA_TYPE       => $column_info->[2]
                , PRECISION       => $column_info->[3]
                , NULLABLE        => $column_info->[4]
                , COLUMN_DEFAULT  => $column_info->[5]
                , TABLE_TYPE      => 'TABLE'
            };
    }

    if ( $progress_bar ) {
        $progress_bar->set_fraction( 0 );
        $progress_bar->set_text( "" );
        $self->kick_gtk;
    }

    return $return;

}

sub fetch_view_list {
    
    my ( $self, $database, $schema ) = @_;
    
    my $sth = $self->prepare(
        "select VIEW_NAME from ALL_VIEWS where OWNER = ?"
    ) || return;
    
    $self->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_view {
    
    my ( $self, $database, $schema, $view_name ) = @_;
    
    my $sth = $self->prepare(
        "select TEXT from ALL_VIEWS where VIEW_NAME = ? and OWNER = ?"
    ) || return;
    
    $self->execute( $sth, [ $view_name, $schema ] )
        || return;
    
    my $row = $sth->fetchrow_arrayref;
    
    return $$row[0];
    
}

sub fetch_function_list {
    
    my ( $self, $database, $schema ) = @_;

    my $sth = $self->prepare(
        "select distinct name as funcname\n"
            . "    from all_source\n"
            . "    where type = 'FUNCTION' and owner = ?"
    ) || return;
    
    $self->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_procedure_list {
    
    my ( $self, $database, $schema ) = @_;

    my $sth = $self->prepare(
        "select distinct name as procname\n"
        . "    from all_source\n"
        . "    where type = 'PROCEDURE' and owner = ?"
    ) || return;
    
    $self->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_all_indexes {

    my ( $self, $database, $schema ) = @_;

    # NOTE: this query will currently only search in the DEFAULT database
    # ( there appears to be no way to include the database in the query )
    # To work around this ( and not switch the current DB underneath other code )
    # we CLONE the connection, "use $database", and then run the query

    my $this_dbh = $self->clone;

    my $sth;

    eval {

        # TODO: ORDER BY INDEX_NAME , ORDINAL_POSITION

        $sth = $this_dbh->prepare(
            qq{
select
            INDEXES.TABLE_NAME
          , INDEXES.INDEX_NAME
          , INDEX_COLUMNS.COLUMN_NAME
          , case
                when INDEXES.UNIQUENESS = 'UNIQUE' then 1
                else 0
            end IS_UNIQUE
          , case
                when CONSTRAINTS.CONSTRAINT_TYPE = 'P' then 1
                else 0
            end IS_PRIMARY
from
            all_indexes       INDEXES
inner join  all_ind_columns   INDEX_COLUMNS
                                            on INDEXES.OWNER = INDEX_COLUMNS.INDEX_OWNER
                                           and INDEXES.INDEX_NAME = INDEX_COLUMNS.INDEX_NAME
left join   user_constraints  CONSTRAINTS
                                            on INDEXES.TABLE_NAME = CONSTRAINTS.TABLE_NAME
                                           and INDEXES.INDEX_NAME = CONSTRAINTS.INDEX_NAME
                                           and CONSTRAINTS.CONSTRAINT_TYPE = 'P'
where INDEXES.OWNER = ?
}
        ) or die( $this_dbh->errstr );

    };

    my $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching indexes"
                , type        => "error"
                , text        => $err
            }
        );

        return;

    }

    print "\n" . $sth->{Statement} . "\n";

    eval {

        $sth->execute( $schema )
            or die( $sth->errstr );

    };

    $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching indexes"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    my $return;

    # We're creating a structure that looks like:

    #$return = {
    #    "INDEX_NAME"  => {
    #        IS_PRIMARY      => 0 or 1
    #      , IS_UNIQUE       => 0 or 1
    #      , TABLE_NAME      => "TABLE_NAME"
    #      , COLUMNS         => [ "COL_1", "COL_2", etc ... ]
    #    }
    #};

    while ( my $row = $sth->fetchrow_hashref ) {

        $return->{ $row->{INDEX_NAME} }->{IS_PRIMARY} = $row->{IS_PRIMARY};
        $return->{ $row->{INDEX_NAME} }->{IS_UNIQUE}  = $row->{IS_UNIQUE};
        $return->{ $row->{INDEX_NAME} }->{TABLE_NAME} = $row->{TABLE_NAME};

        push @{ $return->{ $row->{INDEX_NAME} }->{COLUMNS} }
            , $row->{COLUMN_NAME};

    }

    $this_dbh->disconnect;

    return $return;

}

sub fetch_all_foreign_key_info {

    my ( $self, $database, $schema ) = @_;

    # NOTE: this query will currently only search in the DEFAULT database
    # ( there appears to be no way to include the database in the query )
    # To work around this ( and not switch the current DB underneath other code )
    # we CLONE the connection "use $database", and then run the query

    my $this_dbh = $self->clone;

    my $sth;

    eval {

        $sth = $this_dbh->prepare(
              "select\n"
            . "            CONSTRAINTS.CONSTRAINT_NAME as FOREIGN_KEY_NAME\n"
            . "          , CONSTRAINTS.OWNER as PRIMARY_SCHEMA\n"
            . "          , CONSTRAINTS.TABLE_NAME as PRIMARY_TABLE\n"
            . "          , COLUMNS.COLUMN_NAME as PRIMARY_COLUMN\n"
            . "          , REFERENCED.OWNER as REFERENCED_SCHEMA\n"
            . "          , REFERENCED.TABLE_NAME as REFERENCED_TABLE\n"
            . "          , REFERENCED_COLUMNS.COLUMN_NAME as REFERENCED_COLUMN\n"
            . "from\n"
            . "            all_constraints   CONSTRAINTS\n"
            . "inner join  all_cons_columns  COLUMNS\n"
            . "                                           on CONSTRAINTS.OWNER = COLUMNS.OWNER\n"
            . "                                          and CONSTRAINTS.CONSTRAINT_NAME = COLUMNS.CONSTRAINT_NAME\n"
            . "inner join  all_constraints   REFERENCED\n"
            . "                                           on CONSTRAINTS.R_OWNER = REFERENCED.OWNER\n"
            . "                                          and CONSTRAINTS.R_CONSTRAINT_NAME = REFERENCED.CONSTRAINT_NAME\n"
            . "inner join  all_cons_columns REFERENCED_COLUMNS\n"
            . "                                           on REFERENCED.OWNER = REFERENCED_COLUMNS.OWNER\n"
            . "                                          and REFERENCED.CONSTRAINT_NAME = REFERENCED_COLUMNS.CONSTRAINT_NAME\n"
            . "where\n"
            . "            CONSTRAINTS.CONSTRAINT_TYPE = 'R'\n"
            . "        and CONSTRAINTS.OWNER = ?"
        ) or die( $this_dbh->errstr );

    };

    my $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching foreign key info"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    print "\n" . $sth->{Statement} . "\n";

    eval {

        $sth->execute( $schema )
            or die( $sth->errstr );

    };

    $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching foreign key info"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    my $return;

    while ( my $row = $sth->fetchrow_hashref ) {

        $return->{ $row->{FOREIGN_KEY_NAME} }->{PRIMARY_SCHEMA}    = $row->{PRIMARY_SCHEMA};
        $return->{ $row->{FOREIGN_KEY_NAME} }->{PRIMARY_TABLE}     = $row->{PRIMARY_TABLE};
        $return->{ $row->{FOREIGN_KEY_NAME} }->{REFERENCED_SCHEMA} = $row->{REFERENCED_SCHEMA};
        $return->{ $row->{FOREIGN_KEY_NAME} }->{REFERENCED_TABLE}  = $row->{REFERENCED_TABLE};

        $return->{ $row->{FOREIGN_KEY_NAME} }->{PRIMARY_COLUMN} = $row->{PRIMARY_COLUMN};

        push @{ $return->{ $row->{FOREIGN_KEY_NAME} }->{RELATIONSHIP_COLUMNS} }
            , {
                PRIMARY_COLUMN      => $row->{PRIMARY_COLUMN}
              , REFERENCED_COLUMN   => $row->{REFERENCED_COLUMN}
            };

    }

    $this_dbh->disconnect;

    return $return;

}

sub fetch_all_view_definitions {

    my ( $self, $database, $schema ) = @_;

    my $this_dbh = $self->clone;

    my $sth;

    eval {

        $sth = $this_dbh->prepare(
              "select\n"
            . "      views.VIEW_NAME\n"
            . "    , views.TEXT as VIEW_DEFINITION\n"
            . "from\n"
            . "    all_views views\n"
            . "where\n"
            . "    views.owner = ?"
        ) or die( $this_dbh->errstr );

    };

    my $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching view definitions"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    eval {

        $sth->execute( $schema )
            or die( $sth->errstr );

    };

    $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching view definitions"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    my $return = $sth->fetchall_hashref( 'VIEW_NAME' );

    $this_dbh->disconnect;

    return $return;

}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    # $options contains:
    #{
    #    dont_quote      => 0
    #}

    if ( ! $schema ) {
        if ( $options->{dont_quote} ) {
            return $table;
        } else {
            return '"' . $table . '"';
        }
    } else {
        if ( $options->{dont_quote} ) {
            return $schema . '.' . $table;
        } else {
            return '"' . $schema . '"."' . $table . '"';
        }
    }
    
}

sub drop_db_schema_table_string {

    my ( $self, $database, $schema, $table, $cascade, $options ) = @_;

    my $sql = "drop table " . $self->db_schema_table_string( $database, $schema, $table, $options );

    if ( $cascade ) {
        $sql .= " cascade constraints";
    }

    return $sql;

}

sub limit_clause {
    
    my ( $self, $row_numbers ) = @_;
    
    return "where ROWNUM <= $row_numbers";
    
}

sub limit_select {

    my ( $self, $sql, $row_numbers ) = @_;

    return "select * from\n(\n$sql\n)\nwhere ROWNUM <= $row_numbers";

}

sub object_alias_string {

    my ( $self , $object , $alias ) = @_;

    return "$object $alias"; # Oracle doesn't use the 'as' word in an alias command

}

sub default_object_case {

    my $self = shift;

    return 'upper';

}

1;
