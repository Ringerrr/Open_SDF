package Database::Connection::SQLServer;

use parent 'Database::Connection';

use strict;
use warnings;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'unicode';
use constant LENGTH_FUNCTION    => 'len';
use constant SUBSTR_FUNCTION    => 'substring';

use constant DB_TYPE            => 'SQLServer';

use Glib qw | TRUE FALSE |;

sub new {
    
    my $self = shift->SUPER::new( @_ );
    
    if ( ! $self ) {
        return undef;
    }

    if ( $self->{connection} ) {

        # Needs "use DBD::ODBC;"
        #        DBI->trace(DBD::ODBC->parse_trace_flags('odbcconnection|odbcunicode'));

        $self->{connection}->{LongReadLen} = 65535 * 1024; # 64MB
        warn "long read length: ".$self->{connection}->{LongReadLen};

        $self->{connection}->do( "set textsize 10000000" );
        warn "set textsize: 10000000";

    }

    return $self;
    
}

sub connect_pre {

    my ( $self , $auth_hash , $options_hash ) = @_;

    $auth_hash->{ConnectionString} = undef;

    return ( $auth_hash , $options_hash );

}

sub connection_label_map {

    my $self = shift;

    return {
        Username        => "Username"
      , Password        => "Password"
      , Host_IP         => "Host / IP"
      , Database        => ""
      , Port            => "Port"
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => "ODBC Driver"
    };

}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    if ( ! $auth_hash->{UseBuilder} ) {
        return ( $auth_hash, $auth_hash->{ConnectionString} );
    }
    
    no warnings 'uninitialized';
    
    my $string =
          "dbi:ODBC:"
        . "DRIVER="    . $auth_hash->{ODBC_driver}
        . ";server="   . $auth_hash->{Host}
        . ";Port="     . $auth_hash->{Port};
    
    if ( $auth_hash->{Database} ) {
        $string .= ";database=" . $auth_hash->{Database};
    }
    
    $string .= ";app=SmartDataFramework;use_unicode=true";
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub default_port {

    my $self = shift;

    return 1433;

}

sub connect_post {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    if ( $auth_hash->{Database} ) {
        $self->do( "use " . $auth_hash->{Database} );
    }
    
}


sub fetch_database_list {
    
    my $self = shift;
    
    # these are system databases - we skip them because:
    # 1) i can't think of many reasons why we'd want them visible
    # 2) fetching from the data dictionary while connected to them can fail for normal users
    
    #"select NAME from MASTER..SYSDATABASES where name not in ('master', 'tempdb', 'model', 'msdb')"
    #"select NAME from MASTER..SYSDATABASES"
    
    my $sth = $self->prepare(
        "select NAME from MASTER..SYSDATABASES where name not in ('master', 'tempdb', 'model', 'msdb')"
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_schema_list {
    
    my ( $self, $database ) = @_;
    
    my $sth = $self->prepare(
        "select schema_name from " . $self->db_schema_table_string( $database, "INFORMATION_SCHEMA", "SCHEMATA" )
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;
    
    my $sth = $self->prepare(
        "select TABLE_NAME from " . $self->db_schema_table_string( $database, "INFORMATION_SCHEMA", "TABLES" ) . "\n"
      . "where  TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = ?"
    ) || return;
    
    $self->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_view_list {
    
    my ( $self, $database, $schema ) = @_;
    
    my $sth = $self->prepare(
        "select TABLE_NAME from " . $self->db_schema_table_string( $database,  "INFORMATION_SCHEMA", "TABLES" ) . "\n"
      . "where TABLE_TYPE = 'VIEW' and TABLE_SCHEMA = ?"
    ) || return;
    
    $self->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub limit_clause {
    
    my ( $self, $row_numbers ) = @_;
    
    # SQL Server syntax is "select top N", which is more complex than we can deal with now,
    # so we disable the limit clause ...
    
    return undef;
    
}

sub limit_select {
    
    my ( $self, $sql, $row_numbers ) = @_;
    
    $sql =~ s/^select/select top $row_numbers/;
    
    return $sql;
    
}

sub unicode_function {
    
    my ( $self, $expression ) = @_;
    
    return "master.dbo.fn_varbintohexstr( " . $self->UNICODE_FUNCTION . "( $expression ) )";
    
}

sub fetch_view {
    
    my ( $self, $database, $schema, $view ) = @_;
    
    return;
    
    my $sth = $self->prepare(
        "select DEFINITION from _v_view where VIEWNAME = ?"
    ) || return;
    
    $self->execute( $sth, [ $view ] )
        || return;
    
    my $row = $sth->fetchrow_arrayref;
    
    return $$row[0];
    
}

sub fetch_function_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return;
    
    my $sth = $self->prepare(
        "select FUNCTIONSIGNATURE from _v_function"
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

    if ( ! $self->{cached_procedure_names} ) {

        my $sth = $self->prepare(
            "select routine_name\n"
                . "from   information_schema.routines\n"
                . "where  routine_type = 'PROCEDURE'"
        ) || return;

        $self->execute( $sth )
            || return;

        $self->{cached_procedure_names} = [];

        while ( my $row = $sth->fetchrow_arrayref ) {
            push @{$self->{cached_procedure_names}}, $$row[0];
        }

    }

    return sort( @{$self->{cached_procedure_names}} );
    
}

sub fetch_procedure {

    my ( $self, $database, $schema, $procedure_name ) = @_;

    my $sth = $self->prepare(
        "select routine_definition\n"
            . "from   information_schema.routines\n"
            . "where  routine_type = 'PROCEDURE'"
            . " and   routine_name = ?"
    ) || return;

    $self->execute( $sth, [ $procedure_name ] )
        || return;

    my $row = $sth->fetchrow_arrayref;

    return $$row[0];

}

sub fetch_column_info_array {

    my ( $self, $database, $schema, $table ) = @_;
    
    my $sth = $self->prepare(
        "select\n"
      . "    COLUMN_NAME             as COLUMN_NAME\n"
      . "  , DATA_TYPE               as DATA_TYPE\n"
      . "  , case when upper(DATA_TYPE) like '%CHAR%'     then '(' + cast( CHARACTER_MAXIMUM_LENGTH as VARCHAR(5) ) + ')'\n"
      . "         when upper(DATA_TYPE) like '%BINARY%'   then '(' + cast( CHARACTER_MAXIMUM_LENGTH as VARCHAR(5) ) + ')'\n"
      . "         when upper(DATA_TYPE) = 'DECIMAL' or upper(DATA_TYPE) = 'MONEY'\n"
      . "              then '(' + cast( NUMERIC_PRECISION as VARCHAR(5) ) + ',' + cast( NUMERIC_SCALE as VARCHAR(5) ) + ')'\n"
      . "         else ''\n"
      . "    end                     as PRECISION\n"
      . "  , case when upper(IS_NULLABLE) = 'YES' then 1
                  else 0
             end                     as NULLABLE\n"
      . "from\n"
      . "    " . $self->db_schema_table_string( $database, "INFORMATION_SCHEMA", "COLUMNS" ) . "\n"
      . "where\n"
      . "    TABLE_NAME   = ?\n"
      . "and TABLE_SCHEMA = ?\n"
    ) || return;
    
    $self->execute( $sth, [ $table, $schema ] )
        || return;
    
    my $return;

    while ( my $row = $sth->fetchrow_hashref ) {
        push @{$return}, $row;
    }
    
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
      . "  , case when upper(DATA_TYPE) like '%CHAR%'     then '(' + cast( CHARACTER_MAXIMUM_LENGTH as VARCHAR(5) ) + ')'\n"
      . "         when upper(DATA_TYPE) like '%BINARY%'   then '(' + cast( CHARACTER_MAXIMUM_LENGTH as VARCHAR(5) ) + ')'\n"
      . "         when upper(DATA_TYPE) like 'FLOAT'      then '(' + cast( NUMERIC_PRECISION as VARCHAR(5) ) + ')'\n"
      . "         when upper(DATA_TYPE) IN ('DECIMAL', 'NUMERIC', 'MONEY')\n"
      . "              then '(' + cast( NUMERIC_PRECISION as VARCHAR(5) ) + ',' + cast( NUMERIC_SCALE as VARCHAR(5) ) + ')'\n"
      . "         else ''\n"
      . "    end                     as PRECISION\n"
      . "  , case when upper(IS_NULLABLE) = 'YES' then 1\n"
      . "         else 0\n"
      . "         end                as NULLABLE\n"
      . "  , COLUMN_DEFAULT          as COLUMN_DEFAULT\n"
      . "  , COLUMNPROPERTY(object_id(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY\n"
      . "from\n"
      . "    " . $self->db_schema_table_string( $database, "INFORMATION_SCHEMA", "COLUMNS" ) . "\n"
      . "where\n"
      . "    TABLE_SCHEMA = ?"
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
              , IS_IDENTITY     => $column_info->[6]
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

sub fetch_all_indexes {
    
    my ( $self, $database, $schema ) = @_;
    
    # NOTE: this query will currently only search in the DEFAULT database
    # ( there appears to be no way to include the database in the query )
    # To work around this ( and not switch the current DB underneath other code )
    # we CLONE the connection "use $database", and then run the query
    
    my $this_dbh = $self->clone;
    
    $this_dbh->do( "use $database" );
    
    my $sth;
    
    eval {
        
        $sth = $this_dbh->prepare(
            "select\n"
          . "            upper(TABLES.name)     as TABLE_NAME\n"
          . "          , upper(INDEXES.name)    as INDEX_NAME\n"
          . "          , upper(COLUMNS.name)    as COLUMN_NAME\n"
          . "          , case\n"
          . "                when INDEXES.is_unique = 1 or INDEXES.is_unique_constraint = 1\n"
          . "                then 1\n"
          . "                else 0\n"
          . "            end                    as IS_UNIQUE\n"
          . "          , INDEXES.is_primary_key as IS_PRIMARY\n"
          . "          , is_identity            as IS_IDENTITY\n"
          . "from\n" 
          . "            sys.indexes            INDEXES\n"
          . "inner join  sys.index_columns      INDEX_COLUMNS\n"
          . "                                                     on INDEXES.object_id       = INDEX_COLUMNS.object_id\n"
          . "                                                    and INDEXES.index_id        = INDEX_COLUMNS.index_id\n" 
          . "inner join\n" 
          . "            sys.columns            COLUMNS\n"
          . "                                                    on INDEX_COLUMNS.object_id = COLUMNS.object_id\n"
          . "                                                   and INDEX_COLUMNS.column_id = COLUMNS.column_id\n" 
          . "inner join  sys.tables             TABLES\n"
          . "                                                    on INDEXES.object_id       = TABLES.object_id\n"
          . "inner join  sys.schemas            SCHEMAS\n"
          . "                                                    on TABLES.schema_id        = SCHEMAS.schema_id\n"
          . "where\n"
          . "            SCHEMAS.name = ?\n"
          . "order by\n"
          . "            INDEXES.name\n"
          . "          , INDEXES.index_id\n"
          . "          , INDEX_COLUMNS.index_column_id"
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
        $return->{ $row->{INDEX_NAME} }->{IS_IDENTITY} = $row->{IS_IDENTITY};
        
        push @{ $return->{ $row->{INDEX_NAME} }->{COLUMNS} } , $row->{COLUMN_NAME};

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
    
    $this_dbh->do( "use $database" );
    
    my $sth;
    
    eval {
        
        $sth = $this_dbh->prepare(
            "select\n"
          . "            upper(OBJECTS.name)             as FOREIGN_KEY_NAME\n"
          . "          , upper(PRIMARY_SCHEMA.name)      as PRIMARY_SCHEMA\n"
          . "          , upper(PRIMARY_TABLE.name)       as PRIMARY_TABLE\n"
          . "          , upper(PRIMARY_COLUMNS.name)     as PRIMARY_COLUMN\n"
          . "          , upper(REFERENCED_SCHEMA.name)   as REFERENCED_SCHEMA\n"
          . "          , upper(REFERENCED_TABLE.name)    as REFERENCED_TABLE\n"
          . "          , upper(REFERENCED_COLUMNS.name)  as REFERENCED_COLUMN\n"
          . "from\n"
          . "            sys.foreign_key_columns     FOREIGN_KEY_COLUMNS\n"
          . "inner join  sys.objects                 OBJECTS\n"
          . "                                                                on OBJECTS.object_id            = FOREIGN_KEY_COLUMNS.constraint_object_id\n"
          . "inner join  sys.tables                  PRIMARY_TABLE\n"
          . "                                                                on PRIMARY_TABLE.object_id      = FOREIGN_KEY_COLUMNS.parent_object_id\n"
          . "inner join  sys.schemas                 PRIMARY_SCHEMA\n"
          . "                                                                on PRIMARY_TABLE.schema_id      = PRIMARY_SCHEMA.schema_id\n"
          . "inner join  sys.columns                 PRIMARY_COLUMNS\n"
          . "                                                                on PRIMARY_COLUMNS.column_id    = parent_column_id\n"
          . "                                                               and PRIMARY_COLUMNS.object_id    = PRIMARY_TABLE.object_id\n"
          . "inner join  sys.tables                  REFERENCED_TABLE\n"
          . "                                                                on REFERENCED_TABLE.object_id   = FOREIGN_KEY_COLUMNS.referenced_object_id\n"
          . "inner join  sys.schemas                 REFERENCED_SCHEMA\n"
          . "                                                                on REFERENCED_TABLE.schema_id   = REFERENCED_SCHEMA.schema_id\n"
          . "inner join  sys.columns                 REFERENCED_COLUMNS\n"
          . "                                                                on REFERENCED_COLUMNS.column_id = referenced_column_id\n"
          . "                                                               and REFERENCED_COLUMNS.object_id = REFERENCED_TABLE.object_id\n"
          . "where\n"
          . "            PRIMARY_SCHEMA.name  = ?"
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
          , REFERENCED_COLUMN   => $row->{REFERENCED_COLUMN} # TODO: ???
        };
        
    }
    
    $this_dbh->disconnect;
    
    return $return;
    
}

sub fetch_all_view_definitions {
    
    my ( $self, $database, $schema ) = @_;
    
    my $this_dbh = $self->clone;
    
    $this_dbh->do( "use $database" );
    
    my $sth;
    
    eval {
        
        $sth = $this_dbh->prepare(
            "select\n"
          . "            upper(OBJECTS.name)  as VIEW_NAME\n"
          . "          , MODULES.definition   as VIEW_DEFINITION\n"
          . "from\n"
          . "            sys.objects          OBJECTS\n"
          . "inner join  sys.sql_modules      MODULES\n"
          . "                                             on MODULES.object_id = OBJECTS.object_id\n"
          . "inner join  sys.schemas          SCHEMAS\n"
          . "                                            on OBJECTS.schema_id = SCHEMAS.schema_id\n"
          . "where\n"
          . "            OBJECTS.type = 'V'\n"
          . "and         SCHEMAS.name = ?"
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

sub has_odbc_driver {

    my $self = shift;

    return TRUE;

}

1;
