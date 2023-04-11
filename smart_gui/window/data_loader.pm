package window::data_loader;

use warnings;
use strict;

use parent 'window';

use Glib qw( TRUE FALSE );

use Time::HiRes;
use Text::CSV;
use File::Basename;
#use WWW::Mechanize;
use JSON;
use File::Temp qw / tempfile tempdir /;
use Storable 'dclone';
use File::Slurp;

use constant false => \0;
use constant true => \1;

use Math::BigInt;

use window::data_loader::Connection;

use feature 'switch';

# We use these constants when deciding whether to bump an INT up a scale
use constant    SCALE_BYTEINT     => 0;
use constant    SCALE_TINYINT     => 1;
use constant    SCALE_SMALLINT    => 2;
use constant    SCALE_INT         => 3;
use constant    SCALE_BIGINT      => 4;

use constant    VARCHAR           => 'VARCHAR';
use constant    INT               => 'INT';
use constant    NUMERIC           => 'NUMERIC';
use constant    DATETIME          => 'DATETIME';
use constant    DATE              => 'DATE';
use constant    TIMESTAMP         => 'TIMESTAMP';

my $bigint_lower = '-9223372036854775808';
my $bigint_upper = '9223372036854775807';

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "data_loader"
      , "adjustment1"
      , "adjustment2"
    );

    $self->{load_progress_bar} = $self->{builder}->get_object( "load_progress_bar" );

    $self->{main_stack} = $self->{builder}->get_object( "main_stack" );
    $self->{main_stack_switcher} = Gtk3::StackSwitcher->new();
    $self->{main_stack_switcher}->set_stack( $self->{main_stack} );
    $self->{builder}->get_object( 'HeaderBar' )->pack_end( $self->{main_stack_switcher} );
    $self->{main_stack_switcher}->show;

    $self->{builder}->connect_signals( undef, $self );
    
    $self->{builder}->get_object( "data_loader" )->maximize;
    
    $self->{mem_dbh} = Database::Connection::SQLite->new(
        $self->{globals}
      , {
            location    => ':memory:'
        }
    );
    
    $self->{mem_dbh}->do( "PRAGMA default_synchronous = OFF" );
    
    $self->{target_chooser} = widget::conn_db_table_chooser->new(
        $self->{globals}
      , $self->{builder}->get_object( 'target_chooser_box' )
      , {
            database    => 1
          , schema      => 1
          , table       => 0
        }
      , {
        on_connection_changed     => sub { $self->on_target_connection_changed() }
      , on_database_changed       => sub { $self->regen_tables_list() }
      , on_schema_changed         => sub { $self->regen_tables_list() }
        }
    );
    
    my $model = Gtk3::ListStore->new( "Glib::String" );
    
    foreach my $date_style ( "YMD", "MDY", "DMY", "DYM", "MYD" ) {
        $model->set(
            $model->append
          , 0, $date_style
        );
    }
    
    my $combo = $self->{builder}->get_object( 'DateStyle' );
    $combo->set_model( $model );
    $self->create_combo_renderers( $combo, 0 );
    
    $model = Gtk3::ListStore->new( "Glib::String" );
    
    foreach my $mangler ( "DATETIME_AM-PM" ) {
        $model->set(
            $model->append
          , 0, $mangler
        );
    }
    
    $combo = $self->{builder}->get_object( 'DataMangler' );
    $combo->set_model( $model );
    $self->create_combo_renderers( $combo, 0 );
    
    $self->{progress} = $self->{builder}->get_object( "progress_bar" );
    
    # load last settings ...
    
    my @widget_list = qw | File LinesOfGarbage |;
    
    foreach my $widget ( @widget_list ) {
        $self->manage_widget_value( $widget );
    }
    
    $self->manage_widget_value( "FileContainsHeaders", 1 );
    $self->manage_widget_value( "QuoteChar", '"' );
    $self->manage_widget_value( "Delimiter", "," );
    $self->manage_widget_value( "EscapeChar", "\\" );
    $self->manage_widget_value( "LinesToRead", 10000 );
    $self->manage_widget_value( "NullValue", "\\N" );
    $self->manage_widget_value( "sysbench_template_path" );
    
    $self->{mem_dbh}->do(
        "create table column_defs (\n"
      . "    ID             integer primary key\n"
      . "  , Include        number\n"
      . "  , Name           text\n"
      . "  , Type           text\n"
      . "  , Mangler        text\n"
      . "  , OriginalName   text\n"
      . ")"
    );

    $self->{mem_dbh}->do(
        "create table tables_list (\n"
      . "    ID             integer primary key\n"
      . "  , table_name     text\n"
      . ")"
    );

    $self->{tables_list} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                 => $self->{mem_dbh}
          , column_sorting      => 1
          , sql                 => {
                                        select      => "*"
                                      , from        => "tables_list"
                                      , order_by    => "table_name"
                                   }
          , fields              => [
                                        {
                                            name        => "ID"
                                          , renderer    => "hidden"
                                        }
                                      , {
                                            name        => "table_name"
                                          , x_percent   => 100
                                        }
                                   ]
          , on_row_select       => sub { $self->on_tables_list_select( @_ ) }
          , vbox                => $self->{builder}->get_object( "generate_data_table_list" )
        }
    );

    $self->{definitions_datasheet} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                 => $self->{mem_dbh}
          , column_sorting      => 1
          , sql                 => {
                                        select      => "ID, Include, Name, Type, Mangler, OriginalName"
                                      , from        => "column_defs"
                                      , order_by    => "ID"
                                   }
          , fields              => [
                                        {
                                            name        => "#"
                                          , x_absolute  => 30
                                          , number      => {}
                                        }
                                      , {
                                            name        => "Include"
                                          , x_absolute  => 60
                                          , renderer    => "toggle"
                                        }
                                      , {
                                            name        => "Name"
                                          , x_percent   => 70
                                        }
                                      , {
                                            name        => "Type"
                                          , x_percent   => 30
                                        }
                                      , {
                                            name        => "Mangler"
#                                          , x_percent   => 50
                                          , renderer    => "hidden"
                                        }
                                      , {
                                            name        => "OriginalName"
#                                          , x_percent   => 50
                                          , renderer    => "hidden"
                                        }
                                   ]
                                 
          , vbox                => $self->{builder}->get_object( 'Definitions_Datasheet_box' )
          , auto_tools_box      => TRUE
          , on_row_select       => sub { $self->on_definitions_row_select( @_ ) }
        }
    );
    
    $self->{column_manglers} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                 => $self->{globals}->{local_db}
          , sql                 => {
                                        select      => "ID, exe_sequence, regex_find, regex_replace, description, active"
                                      , from        => "column_manglers"
                                      , order_by    => "exe_sequence"
                                   }
          , fields              => [
                                        {
                                            name        => "ID"
                                          , renderer    => "hidden"
                                        }
                                      , {
                                            name        => "order"
                                          , x_absolute  => 100
                                        }
                                      , {
                                            name        => "regex_find"
                                          , x_percent   => 35
                                        }
                                      , {
                                            name        => "regex_replace"
                                          , x_percent   => 35
                                        }
                                      , {
                                            name        => "description"
                                          , x_percent   => 30
                                        }
                                      , {
                                            name        => "active"
                                          , x_absolute  => 100
                                          , renderer    => "toggle"
                                        }
                                   ]
                                 
          , vbox                => $self->{builder}->get_object( 'regex_manglers_box' )
          , recordset_tools_box => $self->{builder}->get_object( 'regex_manglers_tools_box' )
        }
    );

    $self->manage_widget_value( "dbgen_path" );
    $self->manage_widget_value( "output_path" );
    $self->manage_widget_value( "total_records_to_generate" );
    $self->manage_widget_value( "max_records_per_file" );

    return $self;
    
}

sub regen_tables_list {

    my $self = shift;

    my $connection = $self->{target_chooser}->get_db_connection();
    my $database   = $self->{target_chooser}->get_database_name();
    my $schema     = $self->{target_chooser}->get_schema_name();

    $self->{mem_dbh}->do( "delete from tables_list" );

    my @tables_list = $connection->fetch_table_list( $database , $schema );

    my $insert_sth = $self->{mem_dbh}->prepare( "insert into tables_list ( table_name ) values ( ? )" );

    foreach my $table ( @tables_list ) {
        $insert_sth->execute( $table );
    }

    $self->{tables_list}->query();

}

sub on_find_dbgen_clicked {

    my $self = shift;


    $self->set_widget_value(
        "dbgen_path"
      , $self->file_chooser(
            {
                title   => "Please locate the dbgen binary"
              , action  => "open"
              , type    => "file"
            }
        )
    );

}

sub on_output_path_browser_clicked {

    my $self = shift;

    $self->set_widget_value(
        "output_path"
      , $self->file_chooser(
            {
                title   => "Select the output path ..."
              , action  => "open"
              , type    => "folder"
            }
        )
    );

}

sub on_output_path_changed {

    my $self = shift;

    $self->generate_dbgen_command();

}

sub on_tables_list_select {

    my $self = shift;

    my $connection  = $self->{target_chooser}->get_db_connection();
    my $database    = $self->{target_chooser}->get_database_name();
    my $schema      = $self->{target_chooser}->get_schema_name();
    my $table       = $self->{tables_list}->get_column_value( "table_name" );

    $self->{table}  = $table;

    my $column_info_array = $connection->fetch_column_info_array( $database , $schema , $table );
    my $indexes           = $connection->fetch_all_indexes( $database , $schema );

    # Scan indexes for a primary key index for our table ...
    my @table_pk_columns;

    while ( my ( $index_name , $index_hash ) = each ( %{$indexes} ) ) {
        if ( $index_hash->{TABLE_NAME} eq $table && $index_hash->{IS_PRIMARY} ) {
            @table_pk_columns = @{$index_hash->{COLUMNS}};
            last;
        }
    }

    if ( ! @table_pk_columns ) {
        # If we didn't find a primary key, look for a unique index
        while ( my ( $index_name , $index_hash ) = each ( %{$indexes} ) ) {
            if ( $index_hash->{TABLE_NAME} eq $table && $index_hash->{IS_UNIQUE} ) {
                @table_pk_columns = $index_name->{COLUMNS};
                last;
            }
        }
    }

    my ( @update_cols , @key_cols , @insert_cols );

    my ( @create_cols_strings , @lua_update_types , @lua_key_types , @lua_insert_types
       , @insert_type_lengths , @update_type_lengths , @select_point_type_lengths );

    foreach my $column_info ( @{$column_info_array} ) {

        my ( $this_create_col_string , $lua_type , $type_length_str );

        $this_create_col_string = $column_info->{COLUMN_NAME}  . " " . $column_info->{DATA_TYPE};

        if ( $column_info->{PRECISION} ) {
            $this_create_col_string .= $column_info->{PRECISION};
        }

        if ( ! $column_info->{NULLABLE} ) {
            $this_create_col_string .= " not null";
        }

        if ( defined $column_info->{COLUMN_DEFAULT} ) {
            $this_create_col_string .= "default " . $column_info->{COLUMN_DEFAULT};
        }

        if ( $column_info->{DATA_TYPE} =~ /char/i ) {
            my $length;
            if ( $column_info->{PRECISION} =~ /\(([\d]*)\)/ ) {
                $length = $1;
            }
            $this_create_col_string .= "/*{{ rand.regex('[a-zA-Z ]{" . $length . "}') }}*/";
            $lua_type = "'%s'";
            $type_length_str = "s" . $length;
        } elsif ( $column_info->{SERIAL} ) {
            $this_create_col_string .= "/*{{ rownum }}*/";
            $lua_type = "%d";
            $type_length_str = "i9223372036854775807"; # bigint
        } elsif ( $column_info->{DATA_TYPE} =~ /decimal|numeric/i ) {
            my ( $precision , $scale );
            if ( $column_info->{PRECISION} =~ /\(([\d]*)\,([\d]*)\)/ ) {
                ( $precision , $scale ) = ( $1 , $2 );
            }
            my $l1 = $precision - $scale;
            $this_create_col_string .= "/*{{ rand.regex('[0-9]{" . $l1 . "}') || '.' || rand.regex('[0-9]{" . $scale . "}') }}*/";
            $lua_type = "'%s'";
            $type_length_str = "d" . $l1 . "," . $scale;
        } elsif ( $column_info->{DATA_TYPE} =~ /bigint/i ) {
            $this_create_col_string .= "/*{{ rand.range(0,9223372036854775807) }}*/";
            $lua_type = "%d";
            $type_length_str = "i9223372036854775807";
        } elsif ( $column_info->{DATA_TYPE} =~ /mediumint/i ) {
            $this_create_col_string .= "/*{{ rand.range(0,8388607) }}*/";
            $lua_type = "%d";
            $type_length_str = "i8388607";
        } elsif ( $column_info->{DATA_TYPE} =~ /smallint/i ) {
            $this_create_col_string .= "/*{{ rand.range(0,32767) }}*/";
            $lua_type = "%d";
            $type_length_str = "i32767";
        } elsif ( $column_info->{DATA_TYPE} =~ /tinyint/i ) {
            $this_create_col_string .= "/*{{ rand.range(0,127) }}*/";
            $lua_type = "%d";
            $type_length_str = "i127";
        } elsif ( $column_info->{DATA_TYPE} =~ /int/ ) {
            $this_create_col_string .= "/*{{ rand.range(0,2147483647) }}*/";
            $lua_type = "%d";
            $type_length_str = "i2147483647";
        } elsif ( $column_info->{DATA_TYPE} =~ /text/i ) {
            $this_create_col_string .= "/*{{ rand.regex('[a-zA-Z ]{1000}') }}*/";
            $lua_type = "'%s'";
            $type_length_str = "s1000";
        } elsif ( $column_info->{DATA_TYPE} =~ /double/i ) {
            $this_create_col_string .= "/*{{ rand.regex('[0-9]{6}') || '.' || rand.regex('[0-9]{6}') }}*/";
            $lua_type = "'%s'";
            $type_length_str = "d6,6";
        } elsif ( $column_info->{DATA_TYPE} =~ /datetime|timestamp/ ) {
            $this_create_col_string .= "/*{{ TIMESTAMP WITH TIME ZONE '1000-01-01 00:00:00 UTC' + INTERVAL rand.range(0, 284012524800) SECOND }}*/";
            $lua_type = "'%s'";
            $type_length_str = "t";
        } else {
            $this_create_col_string .= "/* UNSUPPORTED */";
            $lua_type = "UNSUPPORTED";
        }

        push @create_cols_strings , $this_create_col_string;
        $type_length_str = '"' . $type_length_str . '"';

        if ( grep /^$column_info->{COLUMN_NAME}$/ , @table_pk_columns ) {
            push @key_cols , $column_info->{COLUMN_NAME};
            push @lua_key_types , $lua_type;
            push @select_point_type_lengths , $type_length_str;
        } else {
            push @update_cols , $column_info->{COLUMN_NAME};
            push @lua_update_types , $lua_type;
            push @update_type_lengths , $type_length_str;
        }

        if ( ! $column_info->{SERIAL} ) {
            push @insert_cols , $column_info->{COLUMN_NAME};
            push @lua_insert_types , $lua_type;
            push @insert_type_lengths , $type_length_str
        }

    }

    my $create_sql = "create table $table (\n    " . join( "\n  , " , @create_cols_strings ) . "\n)";

    my $select_sql = "select\n            " . join( "\n          , " , @update_cols )
      . "\n        from $table\n"
      . "            where\n                "; # The filter is the same as for the update SQL.

    my $delete_sql = "delete from $table\n            where\n                "; # The filter is the same as for the update SQL.

    my $insert_sql = "insert into $table (\n            " . join( "\n          , " , @insert_cols ) . "\n) values (\n        "
                   . join( " , " , @lua_insert_types ) . "\n)";

    my $update_sql = "update $table set\n            ";

    my @update_col_strings;

    for my $i ( 0 .. @update_cols - 1 ) {
        push @update_col_strings , $update_cols[ $i ] . " = " . $lua_update_types[ $i ];
    }

    $update_sql .= join( "\n          , " , @update_col_strings ) . "\n            where\n    ";

    my @filter_strings;

    for my $i ( 0 .. @key_cols - 1 ) {
        push @filter_strings , $key_cols[ $i ] . " = " . $lua_key_types[ $i ];
    }

    $update_sql .= join( "\nand " , @filter_strings );
    $select_sql .= join( "\nand " , @filter_strings );
    $delete_sql .= join( "\nand " , @filter_strings );

    my $select_lua_fragment = "local selects = {\n\n    {\n\n         [[ " . $select_sql . " ]]\n\n\n"
        . "   , { " . join( " , " , @select_point_type_lengths ) . " }\n\n    }\n\n}\n";

    my $delete_lua_fragment = "local deletes = {\n\n    {\n\n         [[ " . $delete_sql . " ]]\n\n\n"
        . "   , { " . join( " , " , @select_point_type_lengths ) . " }\n\n    }\n\n}\n";

    my $insert_lua_fragment = "local inserts = {\n\n    {\n\n         [[ " . $insert_sql . " ]]\n\n\n"
        . "   , { " . join( " , " , @insert_type_lengths ) . " }\n\n    }\n\n}\n";

    my $update_lua_fragment = "local updates = {\n\n    {\n\n         [[ " . $update_sql . " ]]\n\n\n"
        . "   , { " . join( " , " , @update_type_lengths , @select_point_type_lengths ) . " }\n\n    }\n\n}\n";

    $self->set_widget_value( 'dbgen_table_ddl' , $create_sql );
    $self->set_widget_value( 'sysbench_select' , $select_lua_fragment );
    $self->set_widget_value( 'sysbench_insert' , $insert_lua_fragment );
    $self->set_widget_value( 'sysbench_update' , $update_lua_fragment );
    $self->set_widget_value( 'sysbench_delete' , $delete_lua_fragment );

}

sub on_sysbench_template_path_browse_clicked {

    my $self = shift;

    my $sysbench_template_path = $self->{globals}->{config_manager}->simpleGet( 'data_loader:sysbench_template_path' );

    my $folder = $self->file_chooser(
        {
            title   => "Please select the file to import ..."
          , action  => 'folder'
          , path    => $sysbench_template_path
        }
    ) || return;

    $self->{builder}->get_object( "sysbench_template_path" )->set_text( $folder );

    $self->{globals}->{config_manager}->simpleSet( 'data_loader:sysbench_template_path', $folder );

}

sub on_ExportSysbenchTemplate_clicked {

    my $self = shift;

    my $sysbench_template_path = $self->{globals}->{config_manager}->simpleGet( 'data_loader:sysbench_template_path' );

    if ( ! $sysbench_template_path ) {
        $self->dialog(
            {
                title   => "Select an export path first"
              , type    => "info"
              , text    => "You need to set a path to write template files to first"
            }
        );
        return;
    }

    my $filename = $sysbench_template_path . "/" . $self->{table} . ".lua";

    eval {

        open my $OUTPUT_FH , ">$filename"
            || die( "Error opening export file [$filename]:\n" . $! );

        print $OUTPUT_FH
            $self->get_widget_value( 'sysbench_select' )
          . "\n"
          . $self->get_widget_value( 'sysbench_insert' )
          . "\n"
          . $self->get_widget_value( 'sysbench_update' )
          . "\n"
          . $self->get_widget_value( 'sysbench_delete' )
          . "\n"
          . "return {\n"
          . "    selects=selects\n"
          . "  , inserts=inserts\n"
          . "  , updates=updates\n"
          . "  , deletes=deletes\n"
          . "}\n";

        close $OUTPUT_FH
            || die( "Error closing export file [$filename]:\n" . $! );

    };

    my $err = $@;

    if ( $err ) {
        $self->dialog(
            {
                title   => "Error during export"
              , type    => "warn"
              , text    => "$err"
            }
        );
    }

}

sub generate_dbgen_command {

    my $self = shift;

    $self->{dbgen_ddl_path} = $self->get_widget_value( 'output_path' ) . "/" . $self->{table} . ".sql";

    my @dbgen_cmd_args = (
        $self->get_widget_value( 'dbgen_path' )
      , "-i" , $self->{dbgen_ddl_path}
      , "-o" , $self->get_widget_value( 'output_path' )
      , "-N" , $self->get_widget_value( 'total_records_to_generate' )
      , "-R" , $self->get_widget_value( 'max_records_per_file' )
      , "-r" , 1
      , "-f" , "csv"
      , "--headers"
    );

    my $dbgen_bash_command = join( " \\\n  " , @dbgen_cmd_args );

    $self->set_widget_value( 'dbgen_command' , $dbgen_bash_command );

}

sub on_Write_DDL_and_dbgen_cmd_clicked {

    my $self = shift;

    if ( -e $self->{dbgen_ddl_path} ) {
        my $answer = $self->dialog(
            {
                title   => "Overwrite existing DDL?"
              , type    => "question"
              , "text"=> "File " . $self->{dbgen_ddl_path} . " exists. Overwrite?"
            }
        );
        if ( $answer ne 'yes' ) {
            return;
        }
    }

    eval {
        open TARGET_FILE , ">" . $self->{dbgen_ddl_path}
            || die( "Failed to open target file for writing:\n" . $!) ;
        print TARGET_FILE $self->get_widget_value( 'dbgen_table_ddl' );
        close TARGET_FILE
            || die( "Failed to close target file:\n" . $! );
    };
    my $err = $@;
    if ( $err ) {
        $self->dialog(
            {
                title       => "Error saving file"
              , type        => "error"
              , text        => $err
            }
        );
    }

    eval {
        open TARGET_FILE , ">" . $self->{dbgen_ddl_path} . ".dbgen.sh"
            || die( "Failed to open target file for writing:\n" . $!) ;
        print TARGET_FILE $self->get_widget_value( 'dbgen_command' );
        close TARGET_FILE
            || die( "Failed to close target file:\n" . $! );
    };
    $err = $@;
    if ( $err ) {
        $self->dialog(
            {
                title       => "Error saving file"
              , type        => "error"
              , text        => $err
            }
        );
    }

}

sub on_Load_Generated_Data_clicked {

    my $self = shift;

    my @csv_files;
    my @files = read_dir( $self->get_widget_value( "output_path" ), prefix => 1 ) ;

    my $table = $self->{tables_list}->get_column_value( "table_name" );

    foreach my $file ( @files ) {
        if ( $file =~ /$table\.[\d]*\.csv$/ ) {
            push @csv_files, $file;
        }
    }

    my $count = @csv_files;
    $self->{pulse_fraction} = 1 / $count;

    $self->{suppress_dialogs} = 1;
    $self->{parse_lines} = 1000;

    eval {
        for my $this_csv ( @csv_files ) {
            $self->pulse( "$this_csv" , undef , $self->{load_progress_bar} );
            $self->{builder}->get_object( 'File' )->set_text( $this_csv );
            $self->on_Parse_clicked();
            $self->{builder}->get_object( 'RemoteClient' )->set_active( TRUE );
            $self->on_GenerateExternalTableDDL_clicked();
            $self->on_ImportFromCSV_clicked();
        }
    };

    my $err = $@;
    print( $err );

    $self->{suppress_dialogs} = 0;
    delete $self->{parse_lines};
    
}

sub on_Execute_dbgen_clicked {

    my ( $self ) = @_;

    my $cmd = $self->get_widget_value( 'dbgen_command' );
    my $fw_launcher = $self->open_window( "window::framework_launcher" , $self->{globals} );
    $fw_launcher->execute_cli( $cmd );

}

sub on_target_connection_changed {
    
    my $self = shift;

    $self->{target_db_type} = $self->{target_chooser}->get_db_type;
    
    if ( $self->{target_db_type} eq 'Netezza' ) {
        $self->{builder}->get_object( 'UseDBTargetDefinition' )->show;
    } else {
        $self->{builder}->get_object( 'UseDBTargetDefinition' )->hide;
        $self->{builder}->get_object( 'UseFileDefinition' )->set_active( 1 );
    }
    
    if ( $self->{target_db_type} eq 'Postgres' || $self->{target_db_type} eq 'Greenplum' || $self->{target_db_type} eq 'MySQL' ) {
        $self->{builder}->get_object( 'RemoteClient' )->show;
        $self->{builder}->get_object( 'RemoteClient' )->set_active( 1 );
    } else {
        $self->{builder}->get_object( 'RemoteClient' )->hide;
    }
    
    my $connection_class = $self->{target_db_type};

    $self->{data_loader_class} = window::data_loader::Connection::generate( $self->{globals}, $connection_class, { builder => $self->{builder} } );
    
}

sub on_Browse_clicked {
    
    my $self = shift;
    
    my $last_browsed_dir = $self->{globals}->{config_manager}->simpleGet( 'data_loader:last_browsed_dir' );
    
    my $file = $self->file_chooser(
        {
            title   => "Please select the file to import ..."
          , action  => 'open'
          , type    => 'file'
          , path    => $last_browsed_dir
        }
    ) || return;
    
    $self->{builder}->get_object( "File" )->set_text( $file );
    $self->{builder}->get_object( 'target_table' )->set_text( '' );
    
    my ( $filename, $dir, $ext ) = fileparse( $file );
    
    $self->{globals}->{config_manager}->simpleSet( 'data_loader:last_browsed_dir', $dir );
    
}

sub on_Parse_clicked {
    
    my $self = shift;
    
    $self->{date_delim}         = undef;
    $self->{time_delim}         = undef;
    $self->{column_definitions} = [];
    
    my $file_path           = $self->{builder}->get_object( "File" )->get_text;
    my $column_headers      = 1; # TODO: support files without column headers?
    my $delimiter           = $self->{builder}->get_object( 'Delimiter' )->get_text || ',';
    my $escape_character    = $self->{builder}->get_object( 'EscapeChar' )->get_text;
    my $quote_character     = $self->{builder}->get_object( 'QuoteChar' )->get_text;
    my $encoding            = $self->{builder}->get_object( 'Encoding' )->get_text;
    my $eol                 = $self->{builder}->get_object( 'EOL' )->get_text;
    my $lines_to_read       = exists $self->{parse_lines} ? $self->{parse_lines} : $self->{builder}->get_object( "LinesToRead" )->get_text;
    my $includes_headers    = $self->{builder}->get_object( "FileContainsHeaders" )->get_active;
    my $lines_of_garbage    = $self->{builder}->get_object( "LinesOfGarbage" )->get_text;
    my $null_string         = $self->{builder}->get_object( "NullValue" )->get_text;

    if ( $delimiter eq '\t' ) {
        $delimiter = "\t";
    }
    
    if ( ! -e $file_path ) {
        $self->dialog(
            {
                title       => "File doesn't exist"
              , type        => "error"
              , text        => "File [$file_path] does not exist"
            }
        );
        return;
    }
    
    $self->{globals}->{config_manager}->simpleSet( 'data_loader:last_file', $file_path );
    $self->{globals}->{config_manager}->simpleSet( 'data_loader:quote_char', $quote_character );
    $self->{globals}->{config_manager}->simpleSet( 'data_loader:delimiter', $delimiter );
    $self->{globals}->{config_manager}->simpleSet( 'data_loader:parse_lines', $lines_to_read );
    $self->{globals}->{config_manager}->simpleSet( 'data_loader:includes_headers', $includes_headers );
    $self->{globals}->{config_manager}->simpleSet( 'data_loader:lines_of_garbage', $lines_of_garbage );
    
    # Are we probing for EOL issues?
    # Netezza doesn't like 0x0D at the end of each line, which seems to happen when people
    # export from desktop tools on Windows and / or OSX.
    
    #$self->pulse( "Probing for EOL issues ..." );
    
    my $file_open_string = "<";
    
    if ( $encoding ) {
        $file_open_string .= ":encoding($encoding)";
    }
    
    # TODO: clean up / remove?
    if ( 0 == 1 ) {
        
        open my $probe, $file_open_string, $file_path
            || $self->dialog(
               {
                   title        => "Failed to open file for reading"
                 , type         => "error"
                 , text         => $!
               } );
        
        my $lines_to_probe = 1000;
        my $issue_found = 0;
        
        my $line;
        
        while ( ( $lines_to_probe ) && ( ! $issue_found ) && ( $line = $probe->getline ) ) {
            
            if ( $line =~ /\x0D$/ ) {
                print "Found 0x0D!\n";
                $issue_found = 1;
            }
            
            $lines_to_probe --;
            
        }
        
        $probe->close;
        
        if ( $issue_found ) {
            
            $self->{builder}->get_object( 'EOL_Issues' )->set_active( 1 );
            
            $self->dialog(
                {
                    title       => "EOL issue found"
                  , type        => "warning"
                  , text        => "0x0D characters were detected at the end of lines in this file.\n\n"
                                 . "If you're going to import directly from inside Smart GUI\n"
                                 . "( ie withouth generating a Harvest job ), then please run:\n\n"
                                 . 'sed ' . "'" . 's/\x0D$//' . "'  $file_path > $file_path" . ".fixed\n\n"
                                 . "<b><i>and then load the .fixed file.</i></b>\n\n"
                                 . "If you generate a Harvest job for this file type, an additional step will\n"
                                 . "be generated to convert this file into a Netezza-friendly format as per above.\n\n"
                                 . "<span color='blue'><i>Parsing the current file will now continue ...</i></span>"
                                 
                }
            );
            
        } else {
            
            $self->{builder}->get_object( 'EOL_Issues' )->set_active( 0 );
            
        }
        
    }
    
    my $csv_reader = Text::CSV->new(
        {
            quote_char              => $quote_character
          , binary                  => 1
          , eol                     => ( $eol eq '' ? undef : $eol )
          , sep_char                => $delimiter
#          , escape_char             => ( $escape_character eq '' ? '"' : $escape_character )
          , escape_char             => ( $escape_character eq '' ? undef : $escape_character )
          , quote_space             => 1
          , blank_is_undef          => 1
          , quote_null              => 1
          , always_quote            => 1
          , allow_loose_quotes      => 1
          , undef_str               => $null_string
        }
    );

    if ( ! $csv_reader ) {
        my $err = Text::CSV->error_diag;
        $self->dialog(
            {
                title => "Oh no!"
              , type  => "error"
              , text  => $err
            }
        );
        return TRUE;
    }

    $file_open_string = "<";
    
    if ( $encoding ) {
        $file_open_string .= ":encoding($encoding)";
    }
    
    open my $csv_file, $file_open_string, $file_path
        || $self->dialog(
           {
               title        => "Failed to open file for reading"
             , type         => "error"
             , text         => $!
           } );
    
    # Set the table name if it's not already set
    if ( ! $self->{builder}->get_object( 'target_table' )->get_text ) {
        
        my $table_name;
        
        if ( $file_path =~ /.*\/([\w\-]*)\.[\w]*/ ) {
            
            $table_name = $1;
            $table_name =~ s/\-/_/g;        # dashes
            $table_name =~ s/^(\d)/D_$1/;   # digits starting table names
            
            $self->{builder}->get_object( 'target_table' )->set_text( $table_name );
            
        }
        
    }
    
    $self->{mem_dbh}->do( "delete from column_defs" );
    
    if ( $lines_of_garbage ) {
        
        my $skipped = 0;
        
        while ( $skipped ne $lines_of_garbage ) {
            my $row = $csv_reader->getline( $csv_file );
            $skipped ++;
        } 
        
    }
    
    my $columns = $csv_reader->getline( $csv_file );
    
    if ( $includes_headers ) {
        
        my $column_manglers = $self->{globals}->{local_db}->select(
            "select regex_find, regex_replace from column_manglers where active = 1 order by exe_sequence" );
        
        foreach my $mangler ( @{$column_manglers} ) {
            
            my $find = $mangler->{regex_find};
            my $replace = $mangler->{regex_replace};
            
            my $nameless_col_counter;
            
            foreach my $column ( @{$columns} ) {
                no warnings 'uninitialized';
                if ( ! defined $column || $column eq '' ) {
                    $nameless_col_counter ++;
                    $column = "NAMELESS_COLUMN_" . $nameless_col_counter;
                }
                $column =~ s/$find/$replace/g;
            }
            
        }
        
    }
    
    # Create table for preview
    my ( @preview_col_defs, @placeholders );

    my $counter = 1;

    my $shadow_columns;
    
    foreach my $column ( @{$columns} ) {
        $column =~ s/\s/_/g;
        if ( $includes_headers ) {
            push @preview_col_defs, $column . " text";
        } else {
            push @preview_col_defs, "COLUMN_" . $counter . " text";
            push @{$shadow_columns}, "COLUMN_" . $counter;
        }
        push @placeholders, '?';
        $counter ++;
    }
    
    if ( ! $includes_headers ) {
        $columns = $shadow_columns;
    }
    
    $self->{mem_dbh}->do( "drop table if exists preview" );
    
    my $sql = "create table preview (\n"
      . "    _PREVIEW_ID    integer       primary key , "
      . join( " , ", @preview_col_defs ) . "\n"
      . ")";
    
    $self->{mem_dbh}->do( $sql ) || return;
    
    $sql = "insert into preview (\n" . join( " , ", @{$columns} )
      . "\n) values (\n"
      . join( ' , ', @placeholders )
      . "\n)";
    
    my $insert_preview = $self->{mem_dbh}->prepare( $sql );
    
    my $line_counter = 0;
    
    my $pulse_period = ( int( ( $lines_to_read || 1000 ) / 100 ) ) || 1;
    my $column_counter_memory;
    
    if ( $lines_to_read ) {
        
        while ( my $row = $csv_reader->getline( $csv_file ) ) {
            
            if ( $column_counter_memory && $column_counter_memory != @{$row} ) {
                $self->dialog(
                    {
                        title       => "Column count changed!"
                      , type        => "warning"
                      , text        => "Count count has changed from [$column_counter_memory] to [" . @{$row} . "]"
                    }
                );
            }
            
            $column_counter_memory = @{$row};
            
#            print "This row has " . @{$row} . " columns\n";
            
            my $this_record;
            
            $self->pulse( undef, $pulse_period );
            
            for my $counter ( 0 .. @{$row} - 1 ) {
                
                my $data = $$row[$counter];
                
                if ( defined $data ) {
                    $self->set_column_definition( $$columns[$counter], $counter, $data );
                }
                
#                if ( $line_counter < 20000 ) {
                    push @{$this_record}, $data;
#                }
                
            }
            
            if ( $line_counter < 1000 ) {
                $insert_preview->execute( @{$this_record} );
            }
            
            $line_counter ++;
            
            if ( $line_counter == $lines_to_read ) {
                last;
            }
            
        }
        
    }
    
    my $diag = $csv_reader->error_diag();
    
    if ( ref $diag eq 'Text::CSV::ErrorDiag' ) {
        
        my ( $diag_code, $diag_text, $diag_position ) = @{$diag};
        
        if ( $diag_code != 0 ) {
            
            $self->dialog(
                {
                        title    => "Error parsing CSV"
                      , type     => "error" 
                      , text     => "      Code: [$diag_code]\n"
                                  . "      Text: [$diag_text]\n"
                                  . "  Position: [$diag_position]\n"
                }
            );
            
        } else {

            if ( ! $self->{suppress_dialogs} ) {
                $self->dialog(
                  {
                      title    => "Parsing complete"
                    , type     => "info"
                    , text     => "The CSV appeared to parse correctly ..."
                  }
                );
            }

        }
        
    } elsif ( $diag && $diag ne 'EOF - End of data in parsing input stream' ) {
        
        $self->dialog(
            {
                    title    => "Error parsing CSV"
                  , type     => "error" 
                  , text     => $diag
            }
        );
        
    } else {

        if ( ! $self->{suppress_dialogs} ) {
            $self->dialog(
                {
                    title    => "Parsing complete"
                  , type     => "info"
                  , text     => "The CSV appeared to parse correctly ..."
                }
            );
        }
    }
    
    my $target_connection = $self->{target_chooser}->get_db_connection;
    
    for my $counter ( 0 .. @{$columns} - 1 ) {
        
        my $def = $self->{column_definitions}->[$counter];
        my $type = $self->{data_loader_class}->column_mapper( $def );
        
        $self->{mem_dbh}->do(
            "insert into column_defs ( ID, Include, Name, Type, Mangler, OriginalName ) values ( ?, ?, ?, ?, ?, ? )"
          , [ $counter + 1, 1, $$columns[ $counter ], $type, $def->{mangler}, $$columns[ $counter ] ]
        );
        
    }
    
    $self->{definitions_datasheet}->query;
    
    $self->on_PreviewShowAll_clicked;
    
}

sub on_ShowUniqueValues_clicked {
    
    my $self = shift;
    
    my $column_name = $self->{definitions_datasheet}->get_column_value( "Name" );
    
    if ( exists $self->{preview_datasheet} ) {
        $self->{preview_datasheet}->destroy;
    }
    
    $self->{preview_datasheet} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh             => $self->{mem_dbh}
          , read_only       => TRUE
          , column_sorting  => 1
          , vbox            => $self->{builder}->get_object( 'DataPreviewBox' )
          , fields          => [
                                    {
                                        name        => "unique_values"
                                      , x_percent   => 100
                                    }
                                  , {
                                        name        => "count"
                                      , x_absolute  => 150
                                      , number      => {}
                                    }
                               ]
          , sql             => {
                                    pass_through => "select    $column_name, count(*)\n"
                                                  . "from      preview\n"
                                                  . "where     $column_name is not null\n"
                                                  . "group by  $column_name\n"
                                                  . "order by  count(*) desc"
                               }
        }
    );
    
}

sub on_PreviewShowAll_clicked {
    
    my $self = shift;
    
    my $column_name = $self->{definitions_datasheet}->get_column_value( "Name" );
    
    if ( exists $self->{preview_datasheet} ) {
        $self->{preview_datasheet}->destroy;
    }
    
    $self->{preview_datasheet} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh             => $self->{mem_dbh}
          , read_only       => TRUE
          , vbox            => $self->{builder}->get_object( 'DataPreviewBox' )
          , sql             => {
                                    select      => "*"
                                  , from        => "preview"
                                  , where       => "_PREVIEW_ID < 1001"
                               }
        }
    );
    
}

sub on_ProfileAllColumns_clicked {
    
    my $self            = shift;
    
    my $table_name      = $self->{builder}->get_object( 'target_table' )->get_text;
    my $table_fields    = $self->{mem_dbh}->fetch_field_list( undef, undef, "preview" );
    my @stats           = ();
    
    foreach my $field ( @{$table_fields} ) {
        
        if ( $field ne '_PREVIEW_ID' ) {
            
            my $these_stats_sth = $self->{mem_dbh}->prepare(
                "select $field as VALUE, count(*) as COUNT_OF_FIELD, '$field' as PROFILED_FIELD\n"
              . "from preview\n"
              . "group by $field\n"
              . "having count(*) > 10\n"
              . "order by count(*) desc"
            );
            
            $self->{mem_dbh}->execute( $these_stats_sth );
            
            my $these_stats = $these_stats_sth->fetchall_arrayref;
            
            push @stats, @{$these_stats};
            
        }
        
    }
    
    #print Dumper( \@stats );
    
    use PDF::ReportWriter;
    
    use constant mm     => 72/25.4;     # 25.4 mm in an inch, 72 points in an inch
    
    my $file = "/tmp/" . $table_name . "_column_profiler.pdf";
    unlink $file;
    
    my $fields = [
                    {
                        name                => "Value"
                      , percent             => 85
                      , align               => "left"
                    }
                  , {
                        name                => "Count"
                      , percent             => 15
                      , align               => "right"
                      , format              => {
                                                    separate_thousands  => TRUE
                                               }
                    }
    ];
    
    my $groups = [
                    {
                        name                => "Column"
                      , data_column         => 2
                      , footer_lower_buffer => 10
                      , header              => [
                                                    {
                                                        percent             => 100,
                                                      , text                => "?"
                                                      , colour              => "blue"
                                                      , bold                => TRUE
                                                      , align               => "left"
                                                      , background          => {
                                                                                  shape   => "box"
                                                                                , colour  => "darkgrey"
                                                                               }
                                                      
                                                    }
                                               ]
                      , footer              => [
                                                    {
                                                        percent             => 100,
                                                      , text                => ""
                                                      , bold                => TRUE
                                                      ,                                                       
                                                    }
                                               ]
                    }
    ];
    
    my $page = {
        header  => [
                        {
                            text                => "Data Profiler for file [$table_name]"
                          , colour              => "lightgreen"
                          , percent             => 100
                          , bold                => TRUE
                          , font_size           => 14
                          , align               => "right"
                          , background          => {
                                                      shape   => "box"
                                                    , colour  => "darkgrey"
                                                   }
                        }
                   ]
      , footer  => [
                        {
                            font_size           => 8
                          , text                => "Rendered on \%TIME\%"
                          , align               => 'left'
                          , bold                => FALSE
                          , percent             => 50
                        },
                        {
                            font_size           => 8
                          , text                => "Page \%PAGE\% of \%PAGES\%"
                          , align               => "right"
                          , bold                => FALSE
                          , percent             => 50
                        }
                   ]
    };
    
    my $report_def = {
                        destination                 => $file
                      , paper                       => "A4"
                      , orientation                 => "portrait",
#                      , template                    => $self->{globals}->{reports} . "/billing_all_details_TEMPLATE.pdf"
                      , font_list                   => [ "Times" ]
                      , default_font                => "Times"
                      , default_font_size           => 11
                      , x_margin                    => 10 * mm
                      , upper_margin                => 30 * mm
                      , lower_margin                => 10 * mm
    };
    
    $self->{report} = PDF::ReportWriter->new( $report_def );
    
    my $data = {
        cell_borders            => TRUE
#      , no_field_headers        => TRUE
      , fields                  => $fields
      , groups                  => $groups
      , page                    => $page
      , data_array              => \@stats
    };
    
    $self->{report}->render_data( $data );
    $self->{report}->save;

    $self->{report} = undef;

    $self->open_pdf( $file );
    
}

sub on_definitions_row_select {
    
    my $self = shift;
    
    my $column_name = $self->{definitions_datasheet}->get_column_value( "OriginalName" );
    
    if ( exists $self->{preview_datasheet} ) {
        $self->{preview_datasheet}->destroy;
    }
    
    $self->{preview_datasheet} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh             => $self->{mem_dbh}
          , read_only       => TRUE
          , vbox            => $self->{builder}->get_object( 'DataPreviewBox' )
          , sql             => {
                                    select      => $column_name
                                  , from        => "preview"
                                  , where       => $column_name . " is not null"
                               }
        }
    );
    
}

sub set_column_definition {
    
    my ( $self, $column_name, $counter, $data ) = @_;
    
    my ( $type, $scale );
    
    my $length = length( $data ) || 1;
    
    my $column_def = $self->{column_definitions}->[$counter];
    
    if ( $length > ( $column_def->{max_length} || 0 ) ) {
        $column_def->{max_length} = $length;
    }
    
    if (
            (
                $data !~ /^0/           # We don't want to match things like 00030 ... 
             || $data =~ /^0$/          #  ... but 0 by itself is still ok
            )
          &&    $data =~ /^(\-)?([\d]*)$/
    ) {
        
        $type = INT;
        
        my ( $sign, $number ) = ( $1, $2 );
        
        # avoid warnings, and do proper numeric:numeric comparisons ...
        if ( $number eq '' ) {
            $number = 0;
        }
        
        if (      $number > -128 && $number < 127 ) {
            $scale = SCALE_BYTEINT;
        } elsif ( $number > -32768 && $number < 32767 ) {
            $scale = SCALE_SMALLINT;
        } elsif ( $number > -2147483648 && $number < 2147483647 ) {
            $scale = SCALE_INT;
        } elsif ( $number > $bigint_lower && $number < $bigint_upper ) { # can't use string literals directly, apparently
            $scale = SCALE_BIGINT;
        }
        
    # [^0] = 'match some character that isn't 0' ... not the best start to things ...
#    } elsif ( $data =~ /^[^0]([\d\.])+$/ ) {bigint
    
    } elsif ( $data =~ /^([\d]*)\.([\d]*)$/ ) { # TODO: ip addresses being detected as 'numeric'
        
        $type = NUMERIC;
        
        my ( $left, $right ) = ( $1, $2 );
        
        my $left_digits  = length( $left );
        my $right_digits = length( $right );
        my $precision    = $left_digits + $right_digits;
        
        # NOTE: The terminology is awkward here. I'm ( incorrectly, I know ) using 'scale' to refer to BOTH precision AND scale.
        #       This kinda comes from numeric handling being tacked on after other functionality
        
        $scale = "$precision,$right_digits";
        
    } elsif ( $data =~ /^([\d]+)([\-\/]{1})([\d]+)[\-\/]{1}([\d]+)\s[\d]+(:)?[\d]+:?[\d\.]+\s?(AM|PM)?$/i ) {
        
        my ( $d1, $date_delim, $d2, $d3, $time_delim, $am_pm ) = ( $1, $2, $3, $4, $5, $6 );
        
        if ( ! $self->{date_delim} ) { # prevent updating the widget lots ...
            $self->{date_delim} = $date_delim;
            $self->{builder}->get_object( 'DateDelim' )->set_text( $date_delim );
        }
        
        if ( ! $self->{time_delim} ) { # prevent updating the widget lots ...
            $self->{time_delim} = $time_delim;
            $self->{builder}->get_object( 'TimeDelim' )->set_text( $time_delim );
        }
        
        if ( ! $self->{date_style} ) {
            $self->detect_date_style( $d1, $d2, $d3 );
        }
        
        if ( $am_pm && ! $column_def->{mangler} ) {
            
            $type = VARCHAR;
            $column_def->{max_length} = 25;
            $scale = 25;
            
            # Set a data mangler for this column ...
            $column_def->{mangler} = $self->_mangler_datetime( $column_name );
            
        } else {
            
            $type = DATETIME;
            
        }
        
    } elsif ( $data =~ /^([\d]+)([\-\/]){1}([\d]+)[\-\/]{1}([\d]+)$/ ) {
        
        my ( $d1, $date_delim, $d2, $d3 ) = ( $1, $2, $3, $4 );
        
        if ( ! $self->{date_delim} ) { # prevent updating the widget lots ...
            $self->{date_delim} = $date_delim;
            $self->{builder}->get_object( 'DateDelim' )->set_text( $date_delim );
        }
        
        if ( ! $self->{date_style} ) {
            $self->{date_style} = $self->detect_date_style( $d1, $d2, $d3 );
        }
        
        $type = DATE;
        
    } else {
        
        $type = VARCHAR;
        $scale = $length;
        
    }
    
    if ( ! $column_def->{type} ) {
        
        # 1st time - set up definition
        $column_def->{type} = $type;
        $column_def->{scale} = $scale;
        
    } elsif ( defined $data ) { # don't degrade if we received undef
        
        # Not 1st time - decide whether to overwrite the existing column definition
        if ( $column_def->{type} ne $type && $column_def->{type} ne VARCHAR ) {
            
            if (      $column_def->{type} eq INT ) {
                
                # always degrade an INT
                $column_def = $self->degrade( $column_def, $type, $scale );
                
            } elsif ( $column_def->{type} eq NUMERIC && $type ne INT ) {
                
                # degrade NUMERIC to anything other than an INT
                $column_def = $self->degrade( $column_def , $type , $scale );
                
            } elsif ( $column_def->{type} eq DATE
                   && ( $type eq DATETIME || $type eq VARCHAR )
            ) {
                
                # degrade DATE to DATETIME or VARCHAR
                $column_def = $self->degrade( $column_def , $type , $scale );
                
            } elsif ( $column_def->{type} eq DATETIME && $type eq VARCHAR ) {
                
                # degrade DATETIME to VARCHAR
                $column_def = $self->degrade( $column_def , $type , $scale );
                
            } elsif ( $column_def->{type} ne VARCHAR ) {
                
                # catch-all; anything else indicates a max of types - fallback to VARCHAR
                $column_def = $self->degrade( $column_def , VARCHAR , $scale );
                
            }
            
        }
        
    }
    
    # Finally ... the 'scale' part
    
    # For INTs, check if the scale has increased
    if ( $column_def->{type} eq INT && $type eq INT && $column_def->{scale} < $scale ) {
        $column_def->{scale} = $scale;
    }
    
    # For NUMERICs, we check both precision & scale
    if ( $column_def->{type} eq NUMERIC ) {
        
        # Unpack the definition *and* current 'precision,scale' things,
        # and check if we have to expand either
        
        my ( $def_precision, $def_scale )   = split( ',', $column_def->{scale} );
        my ( $this_precision, $this_scale ) = split( ',', $scale );
        
        my $def_left_digits  = $def_precision - $def_scale;
        my $this_left_digits = $this_precision - $this_scale;
        
        if ( $this_left_digits > $def_left_digits ) {
            $def_left_digits = $this_left_digits;
        }
        
        if ( $this_scale > $def_scale ) {
            $def_scale = $this_scale;
        }
        
        $def_precision = $def_left_digits + $def_scale;
        
        $column_def->{scale} = "$def_precision,$def_scale";
        
    }
    
    $self->{column_definitions}->[$counter] = $column_def;
    
}

sub on_DataManglerApply_clicked {
    
    my $self = shift;
    
    my $column_name = $self->{definitions_datasheet}->get_column_value( "Name" );
    my $mangler = $self->_mangler_datetime( $column_name );
    $self->{definitions_datasheet}->set_column_value( "Mangler", $mangler );
    
}

sub _mangler_datetime {
    
    my ( $self, $column_name ) = @_;
    
    return "case when length($column_name) < 6"
                  . " then NULL"
             . " when upper(substr($column_name,length($column_name),2)) = 'PM'"
                  . " then cast( rtrim(substr($column_name,1,length($column_name)-2)) as datetime ) + interval '12 hours'"
             . " when upper(substr($column_name,length($column_name),2)) = 'AM'"
                  . " then cast( rtrim(substr($column_name,1,length($column_name)-2)) as datetime )"
             . " when length($column_name) < 11" # grrrrrrr sometimes we get DATE only in a DATETIME column
                  . " then cast( rtrim($column_name) || ' 00:00:00' as datetime )"
             . " else cast( $column_name as datetime )"
             . " end as $column_name";
    
}

sub detect_date_style {
    
    my ( $self, $d1, $d2, $d3 ) = @_;
    
    my $date_style;
    
    if ( $d1 > 31 ) {
        
        # looks like YYYY-?-?
        
        if ( $d2 > 12 ) {
            $date_style = 'YDM';
        } elsif ( $d3 > 12 ) {
            $date_style = 'YMD';
        } else {
            return undef;
        }
        
    } elsif ( $d2 > 31 ) {
        
        # looks like ?-YYYY-?
        
        if ( $d1 > 12 ) {
            $date_style = 'DYM';
        } elsif ( $d3 > 12 ) {
            $date_style = 'MYD';
        } else {
            return undef;
        }
        
    } elsif ( $d3 > 31 ) {
        
        # looks like ?-?-YYYY
        
        if ( $d1 > 12 ) {
            $date_style = 'DMY';
        } elsif ( $d2 > 12 ) {
            $date_style = 'MDY';
        } else {
            return undef;
        }
        
    } else {
        
        return undef;
        
    }
    
    $self->set_combo_value( 'DateStyle', $date_style );
    
}

sub degrade {
    
    my ( $self, $def, $type, $scale ) = @_;
    
    $def->{type} = $type;
    
    if ( $type eq VARCHAR ) {
        
        # scale should be the MAX length detected for this column, not the CURRENT scale
        $def->{scale} = $def->{max_length};
        
    } else {
        
        $def->{scale} = $scale;
        
    }
    
    return $def;
    
}

sub on_GenerateExternalTableDDL_clicked {
    
    my $self = shift;
    
    my $ddl;
    
    $self->{definitions_datasheet}->apply;
    
    my $records = $self->{mem_dbh}->select( "select * from column_defs order by ID" );
    
    $ddl = $self->{data_loader_class}->column_definitions_to_ddl( $records );
    
    $self->{builder}->get_object( 'ExternalTableDDL' )->get_buffer->set_text( $ddl );
    
    my $file_path           = $self->{builder}->get_object( 'File' )->get_text;
    my $remote_client       = $self->{builder}->get_object( 'RemoteClient' )->get_active;
    my $delimiter           = $self->{builder}->get_object( 'Delimiter' )->get_text;
    my $encoding            = $self->{builder}->get_object( 'Encoding' )->get_text;
    my $null_value          = $self->{builder}->get_object( 'NullValue' )->get_text;
    my $date_style          = $self->get_combo_value( 'DateStyle' );
    my $date_delim          = $self->{builder}->get_object( 'DateDelim' )->get_text;
    my $quote_char          = $self->{builder}->get_object( 'QuoteChar' )->get_text;
    my $lines_of_garbage    = $self->{builder}->get_object( "LinesOfGarbage" )->get_text;
    my $includes_headers    = $self->{builder}->get_object( "FileContainsHeaders" )->get_active || 0;
    my $escape_char         = $self->{builder}->get_object( "EscapeChar" )->get_text;
    my $eol_char            = $self->{builder}->get_object( "EOL" )->get_text;
    
    my $target_database     = $self->{target_chooser}->get_database_name;
    my $target_schema       = $self->{target_chooser}->get_schema_name;
    my $target_table        = $self->{builder}->get_object( 'target_table' )->get_text;
    
    my $skip_rows           = $lines_of_garbage + $includes_headers;
    
    if ( ! $self->{target_chooser}->get_db_connection->can( 'generate_db_load_command' ) ) {
        $self->dialog(
            {
                title       => "Unsupported database for this function"
              , type        => "error"
              , text        => "Generating a CSV load command not ( yet ) supported for this database type"
            }
        );
        return;
    }
    
    my $options = $self->{target_chooser}->get_db_connection->generate_db_load_command(
        {
            file_path       => $file_path
          , remote_client   => $remote_client
          , null_value      => $null_value
          , delimiter       => $delimiter
          , skip_rows       => $skip_rows
          , quote_char      => $quote_char
          , encoding        => $encoding
          , date_style      => $date_style
          , date_delim      => $date_delim
          , database        => $target_database
          , schema          => $target_schema
          , table           => $target_table
          , escape_char     => $escape_char
          , eol_char        => $eol_char
        }
    );
    
    $self->set_buffer_value(
        $self->{builder}->get_object( 'DBLoaderOptions' )
      , $options
    );
    
}

sub on_ExecuteCreateTable_clicked {
    
    my $self = shift;
    
    my $target_connection       = $self->{target_chooser}->get_db_connection;
    my $target_db               = $self->{target_chooser}->get_database_name;
    my $target_schema           = $self->{target_chooser}->get_schema_name;
    my $target_table            = $self->{builder}->get_object( 'target_table' )->get_text;
    my $table_definition        = $self->get_buffer_value( $self->{builder}->get_object( 'ExternalTableDDL' ) );
    
    $self->{data_loader_class}->create_table( $target_connection, $target_db, $target_schema, $target_table, $table_definition );
    
}

sub on_ImportFromCSV_clicked {
    
    my $self = shift;
    
    my $target_db               = $self->{target_chooser}->get_database_name;
    my $target_schema           = $self->{target_chooser}->get_schema_name;
    my $target_table            = $self->{builder}->get_object( 'target_table' )->get_text;
    my $table_definition        = $self->get_buffer_value( $self->{builder}->get_object( 'ExternalTableDDL' ) );
    my $db_loader_options       = $self->get_buffer_value( $self->{builder}->get_object( 'DBLoaderOptions' ) );
    my $remote_client           = $self->{builder}->get_object( 'RemoteClient' )->get_active;
    my $file_path               = $self->{builder}->get_object( 'File' )->get_text;
    
    $self->{target_chooser}->get_db_connection->load_csv(
        {
            mem_dbh             => $self->{mem_dbh}
          , target_db           => $target_db
          , target_schema       => $target_schema
          , target_table        => $target_table
          , table_definition    => $table_definition
          , copy_command        => $db_loader_options
          , remote_client       => $remote_client
          , file_path           => $file_path
          , progress_bar        => $self->{progress}
          , suppress_dialog     => ( exists $self->{suppress_dialogs} && $self->{suppress_dialogs} ) ? 1 : 0
        }
    );
    
}

sub on_Preview_DataToCSV_clicked {
    
    my $self = shift;
    
    $self->{preview_datasheet}->data_to_csv;
    
}

sub on_GenerateHarvestJob_clicked {
    
    my $self = shift;
    
    my $processing_group    = $self->{builder}->get_object( 'HarvestGroupName' )->get_text;
    my $staging_schema      = $self->{builder}->get_object( 'StagingSchemaName' )->get_text;
    
    my $target_connection   = $self->{target_chooser}->get_connection_name;
    my $target_database     = $self->{target_chooser}->get_database_name;
    my $target_schema       = $self->{target_chooser}->get_schema_name;
    
    my $target_table        = $self->{builder}->get_object( 'target_table' )->get_text;
    
    if ( ! $processing_group || ! $target_connection || ! $target_database || ! $target_schema || ! $target_table ) {
        
        $self->dialog(
            {
                title       => "Incomplete setup"
              , type        => "error"
              , text        => "Please complete the setup ... you need a processing group name, and target information ..."
            }
        );
        
        return;
        
    }
    
    my $load_definition = $self->get_buffer_value( 'ExternalTableDDL' );
    
    if ( ! $load_definition ) {
        
        $self->dialog(
            {
                title       => "No load options"
              , type        => "error"
              , text        => "You need to generate load options first ..."
            }
        );
        
        return;
        
    }
    
    my $lines_of_garbage    =  $self->{builder}->get_object( "LinesOfGarbage" )->get_text;
    my $includes_headers    =  $self->{builder}->get_object( "FileContainsHeaders" )->get_active || 0;
    my $skip_rows           =  $lines_of_garbage + $includes_headers;
    
    my $options = {
        processing_group    => $processing_group
      , staging_schema      => $staging_schema
      , target_connection   => $target_connection
      , target_database     => $target_database
      , target_schema       => $target_schema
      , target_table        => $target_table
      , load_definition     => $load_definition
      , delimiter           => $self->{builder}->get_object( 'Delimiter' )->get_text
      , encoding            => $self->{builder}->get_object( 'Encoding' )->get_text
      , null_value          => $self->{builder}->get_object( 'NullValue' )->get_text
      , date_style          => $self->get_combo_value( 'DateStyle' )
      , date_delim          => $self->{builder}->get_object( 'DateDelim' )->get_text
      , time_delim          => $self->{builder}->get_object( 'TimeDelim' )->get_text
      , quote_char          => $self->{builder}->get_object( 'QuoteChar' )->get_text
      , lines_of_garbage    => $lines_of_garbage
      , includes_headers    => $includes_headers
      , escape_character    => $self->{builder}->get_object( 'EscapeChar' )->get_text
      , eol_issues          => ( $self->{builder}->get_object( "EOL_Issues" )->get_active || 0 )
      , skip_rows           => $skip_rows
    };
    
    my $target_connection_object = $self->{target_chooser}->get_db_connection;
    
    $target_connection_object->generate_harvest_job( $options );
    
}

sub on_data_loader_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
