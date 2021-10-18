package Database::Connection::MySQL;

use parent 'Database::Connection';

use strict;
use warnings;

use feature 'switch';

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'hex';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'MySQL';

use Glib qw | TRUE FALSE |;

sub default_port {
    
    my $self = shift;
    
    return 3306;
    
}

sub connect_pre {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    # We *always* rebuild the connection string for MySQL, as we have to
    # include the database in the connection string. Do do this ( it happens in
    # connect_do() ... we just blank out the existing string ...
    
    $auth_hash->{ConnectionString} = undef;
    
    $options_hash->{db_options_hash} = {
        RaiseError        => 0
      , AutoCommit        => 1
      , mysql_enable_utf8 => 1
    };
    
    return ( $auth_hash , $options_hash );
    
}

sub connect_post {
    
    my ( $self ) = @_;
    
    my $sth = $self->prepare( 'select @@BASEDIR as BASEDIR' )
        || return;

    $self->execute( $sth )
        || return;

    my $row = $sth->fetchrow_hashref;

    if ( $row->{BASEDIR} =~ /rdsdbbin/ ) {
        $self->{is_rds} = 1;
    } else {
        $self->{is_rds} = 0;
    }

    #$self->{connection}->{FetchHashKeyName} = "NAME_uc";
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    no warnings 'uninitialized';
    
    my $string =
          "dbi:mysql:"
        . "database="  . ( $auth_hash->{Database} || 'information_schema' )
        . ";host="       . $auth_hash->{Host}
        . ";port="      . ( $auth_hash->{Port} || 3306 )
        . ";mysql_local_infile=1" # Needed to be able to execute 'load data infile' for TiDB
        . ";mysql_use_result=1";  # prevent $dbh->execute() from pulling all results into memory
    
    if ( $auth_hash->{Attribute_1} ) {
        $string .= ";mysql_ssl=1"
    }
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub connection_label_map {

    my $self = shift;

    return {
        Username        => "Username"
      , Password        => "Password"
      , Database        => ""
      , Host_IP         => "Host / IP"
      , Port            => "Port"
      , Attribute_1     => "Use SSL"
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };

}

sub fetch_database_list {
    
    my $self = shift;
    
    my $sth = $self->prepare( "show databases" )
        || return;
    
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
        "select TABLE_NAME from information_schema.TABLES where TABLE_TYPE like 'BASE TABLE' and TABLE_SCHEMA like ?"
    ) || return;

    $self->execute( $sth, [ $database ] )
        || return;

    my @return;

    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }

    return sort( @return );
    
}

sub fetch_view_list {
    
    my ( $self, $database ) = @_;
    
    my $sth = $self->prepare(
        "select TABLE_NAME from information_schema.TABLES where TABLE_TYPE like '%VIEW%' and TABLE_SCHEMA like ?"
    ) || return;
    
    $self->execute( $sth, [ $database ] )
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
        "show create view " . $database . "." . $view_name
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my $row = $sth->fetchrow_arrayref;
    
    return $$row[1];
    
}

sub fetch_function {

    my ( $self, $database, $schema, $function_name ) = @_;

    my $sth = $self->prepare(
        "show create function " . $database . "." . $function_name
    ) || return;

    $self->execute( $sth )
        || return;

    my $row = $sth->fetchrow_arrayref;

    return $$row[2];

}

sub fetch_procedure {

    my ( $self, $database, $schema, $procedure_name ) = @_;

    my $sth = $self->prepare(
        "show create procedure " . $database . "." . $procedure_name
    ) || return;

    $self->execute( $sth )
        || return;

    my $row = $sth->fetchrow_arrayref;

    return $$row[2];

}

sub fetch_function_list {
    
    my ( $self, $database, $schema ) = @_;

    my $sth = $self->prepare(
        "select ROUTINE_NAME from INFORMATION_SCHEMA.ROUTINES\n"
      . "where  ROUTINE_SCHEMA = ? and ROUTINE_TYPE = 'FUNCTION'"
    ) || return;
    
    $self->execute( $sth, [ $database ] )
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
        "select ROUTINE_NAME from INFORMATION_SCHEMA.ROUTINES\n"
      . "where  ROUTINE_SCHEMA = ? and ROUTINE_TYPE = 'PROCEDURE'"
    ) || return;
    
    $self->execute( $sth, [ $database ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $sth = $self->prepare( "show columns from $table" )
        || return;
    
    $self->execute( $sth )
        || return;
    
    my $mysql_hash = $sth->fetchall_hashref( "Field" ); # was FIELD
    
    my $return;
    
    # Note: we use $dbh->{FetchHashKeyName} = "NAME_uc" in the constructor
    # to try to force column names to upper-case, but this doesn't appear
    # to completely work, so we further mangle things here ...
    
    foreach my $column_name ( keys %{$mysql_hash} ) {
        
        my $data_type = $mysql_hash->{$column_name}->{Type};
        my $precision_scale;
        
        if ( $data_type =~ /([\w\s]*)(\(.*\))/ ) {
            ( $data_type , $precision_scale ) = ( $1 , $2 );
        }
        
        $return->{ uc($column_name) } = {
            COLUMN_NAME     => $column_name
          , DATA_TYPE       => $data_type
          , PRECISION       => $precision_scale
          , NULLABLE        => $mysql_hash->{$column_name}->{Null} eq 'YES' ? 1 : 0
        };
    }
    
    if ( $options->{force_upper} ) {
        $return = $self->mangle_case_result_data( $return, "upper" );
    }
    
    return $return;
    
}

sub fetch_all_indexes {

    my ( $self, $database, $schema ) = @_;

    my $this_dbh = $self->connection;

    my $sth;

    # MySQL splits the availability of ( PRIMARY , UNIQUE ) indexes ( constraints, I guess )
    # from the rest of the indexes. We need to run 2 queries to fetch them all.

    # Primary and Unique:
    eval {

        $sth = $this_dbh->prepare(
            "select\n"
          . "    CONSTRAINT_NAME\n"
          . "  , TABLE_NAME\n"
          . "  , case when constraint_type = 'PRIMARY KEY' then 1 else 0 end as IS_PRIMARY\n"
          . "  , case when constraint_type = 'UNIQUE'      then 1 else 0 end as IS_UNIQUE\n"
          . "  , COLUMN_NAME\n"
          . "from\n"
          . "        information_schema.table_constraints t\n"
          . "join    information_schema.key_column_usage  k\n"
          . "                                                 using ( constraint_name , table_schema , table_name )\n"
          . "where\n"
          . "        t.constraint_type in ( 'PRIMARY KEY' , 'UNIQUE' )\n"
          . "and     t.table_schema = ?\n"
          . "order by\n"
          . "        table_name , constraint_name , ordinal_position"
        ) or die( $this_dbh->errstr );

    };

    my $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching constraints"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    print "\n" . $sth->{Statement} . "\n";

    eval {

        $sth->execute( $database )
            or die( $sth->errstr );

    };

    $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching constraints"
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

        # MySQL uses the constraint name of 'PRIMARY' for EVERY primary key constraint,
        # and otherwise doesn't seem to enforce unique constraint names - they are instead
        # unique PER TABLE. This won't work for us, so we shove the table name
        # in front of the constraint name.

        my $constraint_name = $row->{TABLE_NAME} . "_" . $row->{CONSTRAINT_NAME};

        $return->{ $constraint_name }->{IS_PRIMARY} = $row->{IS_PRIMARY};
        $return->{ $constraint_name }->{IS_UNIQUE}  = $row->{IS_UNIQUE};
        $return->{ $constraint_name }->{TABLE_NAME} = $row->{TABLE_NAME};

        push @{ $return->{ $constraint_name }->{COLUMNS} }
            , $row->{COLUMN_NAME};

    }

    return $return;

}

sub fetch_all_foreign_key_info {

    my ( $self, $database, $schema ) = @_;
    
    my $sth;
    
    eval {
       
        $sth = $self->prepare(
qq{
select
        constraint_name                        as FOREIGN_KEY_NAME
      , table_schema                           as PRIMARY_SCHEMA
      , table_name                             as PRIMARY_TABLE
      , referenced_table_schema                as REFERENCED_SCHEMA
      , referenced_table_name                  as REFERENCED_TABLE
      , column_name                            as PRIMARY_COLUMN
      , referenced_column_name                 as REFERENCED_COLUMN
from    information_schema.key_column_usage
where
        table_schema = ?
and     referenced_table_name is not null
order by
        table_schema
      , constraint_name
      , ordinal_position
}
        ) || return;
        
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

        $sth->execute( $database )
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

        # $return->{ $row->{FOREIGN_KEY_NAME} }->{PRIMARY_COLUMN} = $row->{PRIMARY_COLUMN};

        push @{ $return->{ $row->{FOREIGN_KEY_NAME} }->{RELATIONSHIP_COLUMNS} }
            , {
                PRIMARY_COLUMN      => $row->{PRIMARY_COLUMN}
              , REFERENCED_COLUMN   => $row->{REFERENCED_COLUMN}
            };
        
    }

    return $return;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    # $options contains:
    #{
    #    dont_quote      => 0
    #}

    if ( $database ) {
        if ( $options->{dont_quote} ) {
            return $database . '.' . $table;
        } else{
            return '`' . $database . '`.`' . $table . '`';
        }
    } else {
        if ( $options->{dont_quote} ) {
            return $table;
        } else{
            return '`' . $table . '`';
        }
    }
    
}

sub has_schemas {
    
    my $self = shift;
    
    return 0;
    
}

sub generate_db_load_command {
    
    my ( $self, $options ) = @_;
    
    # options looks like:
#    {
#        file_path       => $file_path
#      , remote_client   => $remote_client
#      , null_value      => $null_value
#      , delimiter       => $delimiter
#      , skip_rows       => $skip_rows
#      , quote_char      => $quote_char
#      , encoding        => $encoding
#      , date_style      => $date_style
#      , date_delim      => $date_delim
#      , escape_char     => $escape_char
#      , eol_char        => $eol_char
#      , database        => $target_database
#      , schema          => $target_schema
#      , table           => $target_table
#    }
    
    # Load command looks like ( for the MySQL docs ):
    
#    LOAD DATA [LOW_PRIORITY | CONCURRENT] [LOCAL] INFILE 'file_name'
#    [REPLACE | IGNORE]
#    INTO TABLE tbl_name
#    [PARTITION (partition_name,...)]
#    [CHARACTER SET charset_name]
#    [{FIELDS | COLUMNS}
#        [TERMINATED BY 'string']
#        [[OPTIONALLY] ENCLOSED BY 'char']
#        [ESCAPED BY 'char']
#    ]
#    [LINES
#        [STARTING BY 'string']
#        [TERMINATED BY 'string']
#    ]
#    [IGNORE number {LINES | ROWS}]
#    [(col_name_or_user_var,...)]
#    [SET col_name = expr,...]
    
    my $load_command = "load data ";
    
    if ( $options->{remote_client} ) {
        $load_command .= "local "; # the local keyword causes MySQL to load from a remote client
    }
    
    $load_command   .= "infile '" . $options->{file_path} . "'\n"
                     . "into table " . $self->db_schema_table_string( $options->{database}, $options->{schema}, $options->{table} ) . "\n"
                     . "columns\n"
                     . "    terminated by '" . $options->{delimiter} . "'\n"
                     . "    optionally enclosed by '" . $options->{quote_char} . "'\n"
    ;
    
    if ( $options->{eol_char} ) {
        $load_command .= "lines terminated by '" . $options->{eol_char} . "'\n";
    }
    
    if ( $options->{skip_rows} ) {
        $load_command .= "ignore " . $options->{skip_rows} . " lines\n";
    }
    
    return $load_command;

}

sub load_csv {
    
    # This method loads data, and is called from window::data_loader
    
    my ( $self, $options ) = @_;
    
    # options:
    #{
    #    mem_dbh             => $mem_dbh                - not in use for MySQL
    #  , target_db           => $target_db              - not in use for MySQL
    #  , target_schema       => $target_schema          - not in use for MySQL
    #  , target_table        => $target_table           - not in use for MySQL
    #  , table_definition    => $table_definition       - not in use for MySQL
    #  , copy_command        => $copy_command
    #  , remote_client       => $remote_client
    #  , file_path           => $file_path
    #  , progress_bar        => $progress_bar
    #  , suppress_dialog     => $suppress_dialog
    #}
    
    print( "\n$options->{copy_command}\n" );
    
    my $start_ts = Time::HiRes::gettimeofday;
    
    my $records;
    my $csv_file;
    
    $records = $self->do( $options->{copy_command} ) || return;
    
    my $end_ts   = Time::HiRes::gettimeofday;
    
    if ( ! $options->{suppress_dialog} ) {
        
        $self->dialog(
            {
                title       => "Import Complete"
              , type        => "info"
              , text        => "[$records] records inserted in " . ( $end_ts - $start_ts ) . " seconds\n"
                             . "You can use the 'browser' window ( menu entry from the main window ) to view the data ..."
            }
        );
        
    }
    
    return TRUE;
    
}

sub generate_current_activity_query {
    
    my $self = shift;
    
    #return "select\n"
    #     . "    pid, state, query\n"
    #     . "from\n"
    #     . "    pg_stat_activity";
    
    return "select\n"
         . "    User        as username\n"
         . "  , db          as db\n"
         . "  , Host        as host\n"
         . "  , Id          as id\n"
         . "  , State       as state\n"
         . "  , Info        as query\n"
         . "from\n"
         . "    information_schema.processlist\n"
         . "where\n"
         . "    command != 'Sleep'";
         
}

sub generate_query_cancel_sql {

    my ( $self, $pid ) = @_;

    if ( ! $self->{is_rds} ) {
        return "kill query $pid";
    } else {
        return "call mysql.rds_kill_query($pid)";
    }

}

sub generate_session_kill_sql {

    my ( $self, $pid ) = @_;

    if ( ! $self->{is_rds} ) {
        return "kill connection $pid";
    } else {
        return "call mysql.rds_kill($pid)";
    }

}

1;
