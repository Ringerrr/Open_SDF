package Database::Connection::Hive;

use parent 'Database::Connection';

use strict;
use warnings;

use feature 'switch';

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'hex';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'Hive';

use Glib qw | TRUE FALSE |;

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
      , ODBC_driver     => "ODBC Driver"
    };
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    no warnings 'uninitialized';
    
    my $string =
          "dbi:ODBC:"
        . "DRIVER="               . $auth_hash->{ODBC_driver}
        . ";Host="                . $auth_hash->{Host}
        . ";Port="                . $auth_hash->{Port};
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub default_port {

    my $self = shift;

    return -1;

}

sub connect_post {
    
    my ( $self, $auth_hash, $options_hash ) = @_;
    
    if ( $auth_hash->{Database} ) {
        $self->{connection}->do( "use " . $auth_hash->{Database} );
    }
    
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
        "show tables"
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
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
    
    my $column_info_array;
    
    if ( ! $self->{_column_info_cache}->{$database}->{$table} ) {
        
        my $sth = $self->prepare( "describe $table" )
            || return;
        
        $self->execute( $sth )
            || return;
        
        while ( my $column_info = $sth->fetchrow_hashref ) {
            if ( $column_info->{col_name} eq '' ) { # the column info is followed by a blank line, then partition info
                last;
            }
            push @{$column_info_array}, $column_info;
        }
        
        $self->{_column_info_cache}->{$database}->{$table} = $column_info_array;
        
    }
    
    return $self->{_column_info_cache}->{$database}->{$table};
    
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

sub has_odbc_driver {

    my $self = shift;

    return TRUE;

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
        $load_command .= "ignore " . $options->{skip_rows} . " rows\n";
    }
    
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
         . "  , Id          as pid\n"
         . "  , State       as state\n"
         . "  , Info        as query\n"
         . "from\n"
         . "    information_schema.processlist";
         
}

1;
