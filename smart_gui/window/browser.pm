package window::browser;

use warnings;
use strict;

use parent 'window';

use Glib qw( TRUE FALSE );

use Time::HiRes;
use Data::Dumper;
use File::Basename;
use PDF::ReportWriter;

use feature     'switch';

use constant    TYPE_COLUMN                 => 0;
use constant    OBJECT_COLUMN               => 1;
use constant    EXTRA_1_COLUMN              => 2;
use constant    EXTRA_2_COLUMN              => 3;

use constant    CONNECTION_TYPE             => 'CONNECTION';
use constant    DATABASE_TYPE               => 'DATABASE';
use constant    SCHEMA_COLLECTION_TYPE      => 'SCHEMA_COLLECTION';
use constant    SCHEMA_TYPE                 => 'SCHEMA';
use constant    TABLE_COLLECTION_TYPE       => 'TABLE_COLLECTION';
use constant    TABLE_TYPE                  => 'TABLE';
use constant    COLUMN_TYPE                 => 'COLUMN';
use constant    VIEW_COLLECTION_TYPE        => 'VIEW_COLLECTION';
use constant    VIEW_TYPE                   => 'VIEW';
use constant    MVIEW_COLLECTION_TYPE       => 'MVIEW_COLLECTION';
use constant    MVIEW_TYPE                  => 'MVIEW';
use constant    FUNCTION_COLLECTION_TYPE    => 'FUNCTION_COLLECTION';
use constant    FUNCTION_TYPE               => 'FUNCTION';
use constant    PROCEDURE_COLLECTION_TYPE   => 'PROCEDURE_COLLECTION';
use constant    PROCEDURE_TYPE              => 'PROCEDURE';

use constant    COLUMN_EXPRESSION_LENGTH    => 53;

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "browser"
    );
    
    $self->{builder}->connect_signals( undef, $self );
    
    $self->{mem_dbh} = Database::Connection::SQLite->new(
        $self->{globals}
      , {
            location    => ":memory:"
        }
    );
    
    #$self->kick_gtk;
    
    $self->{builder}->get_object( 'main_paned' )->set_position( $self->{globals}->{config_manager}->simpleGet( 'browser::horizontal_pane' ) || 400 );
    
    my $header_bar = $self->{builder}->get_object( 'HeaderBar' );
    $header_bar->set_title( "Smart Associates Database Explorer" );
    $header_bar->set_show_close_button( TRUE );
    
    my $icon_folder = $self->{globals}->{paths}->{app} . "/icons";
    
    # Buttons
    
    my $button_control = [
        {
            text        => "New SQL page\n\nCTRL + t"
          , method      => 'add_blank_page'
          , icon        => 'tab-new'
        }
      , {
            text        => "Load from File\n\nCTRL + o"
          , method      => 'load_page_from_file'
          , icon        => 'document-open'
        }
      , {
            text        => "Save Current Page\n\nCTRL + s"
          , method      => 'save_current_page'
          , icon        => 'document-save'
        }
      , {
            text        => "Open Application Configuration"
          , method      => 'on_configuration_clicked'
          , icon        => 'preferences-system'
        }
    ];
    
    foreach my $button_def ( @{$button_control} ) {
    
        my $button = Gtk3::Button->new;
        my $icon   = Gtk3::Image->new_from_icon_name( $button_def->{icon}, 'button' );
        $button->set_image( $icon );
        $button->set( 'always-show-image', TRUE );
        $button->set_tooltip_markup( $button_def->{text} );
        my $method_name = $button_def->{method};
        $button->signal_connect( 'button-press-event', sub { $self->$method_name() } );

        $header_bar->pack_start( $button );
        
    }
    
    $header_bar->show_all;
    
    $self->{builder}->get_object( "browser" )->maximize;
    
    $self->{progress} = $self->{builder}->get_object( "progress_bar" );
    
    # the tree
    
    my $treeview = $self->{builder}->get_object( 'browser_tree' );
    
    my $renderer = Gtk3::CellRendererPixbuf->new;
    
    my $column = Gtk3::TreeViewColumn->new_with_attributes(
        ""
      , $renderer
    );
    
    $column->set_cell_data_func( $renderer, sub { $self->render_pixbuf_cell( @_ ); } );
    
    $treeview->append_column( $column );
    
    # 1st visible column
    my $col_1_renderer = Gtk3::CellRendererText->new;
    
    # TODO: scaling of text and icons based on config?
    #$col_1_renderer->set( 'scale', 0.9 );
    
    $self->{OBJECT_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
        "items",
        $col_1_renderer,
        'text'  => OBJECT_COLUMN
    );
    
    $treeview->append_column( $self->{OBJECT_COLUMN} );
    
    $self->{OBJECT_COLUMN}->{_percent} = 100;
    $self->{OBJECT_COLUMN}->{_renderer} = $col_1_renderer;
    
    $treeview->signal_connect( button_press_event => sub { $self->on_tree_click( @_ ) } );
    $treeview->signal_connect( 'key_press_event'      => sub { $self->on_tree_key_press_event( @_ ) } );

    # render icons ...
    # this *should* be as easy as:
    # $self->{icons}->{CONNECTION} = Gtk3::Gdk::Pixbuf->new_from_file_at_size( $icon_folder . "/connection.png", 24, 24 );
    #  ... but unfortunately at the moment there are issues with the Gtk3::Gdk bindings.
    # Luckily, there is a hacky work-around - we create a Gtk3::Image and use the get_pixbuf() method on it.
    
    $self->{icons}->{CONNECTION}            = $self->to_pixbuf( $icon_folder . "/connection_32x32.png" );
    $self->{icons}->{DATABASE}              = $self->to_pixbuf( $icon_folder . "/database_32x32.png" );
    $self->{icons}->{SCHEMA}                = $self->to_pixbuf( $icon_folder . "/schema_32x32.png" );
    $self->{icons}->{SCHEMA_COLLECTION}     = $self->to_pixbuf( $icon_folder . "/schema_32x32.png" );
    $self->{icons}->{TABLE}                 = $self->to_pixbuf( $icon_folder . "/table_32x32.png" );
    $self->{icons}->{TABLE_COLLECTION}      = $self->{icons}->{TABLE};
    $self->{icons}->{COLUMN}                = $self->to_pixbuf( $icon_folder . "/column_32x32.png" );
    $self->{icons}->{VIEW}                  = $self->to_pixbuf( $icon_folder . "/view_32x32.png" );
    $self->{icons}->{VIEW_COLLECTION}       = $self->{icons}->{VIEW};
    $self->{icons}->{MVIEW}                 = $self->to_pixbuf( $icon_folder . "/mview_32x32.png" );
    $self->{icons}->{MVIEW_COLLECTION}      = $self->{icons}->{MVIEW};
    $self->{icons}->{FUNCTION}              = $self->to_pixbuf( $icon_folder . "/function_32x32.png" );
    $self->{icons}->{FUNCTION_COLLECTION}   = $self->{icons}->{FUNCTION};
    $self->{icons}->{PROCEDURE}             = $self->to_pixbuf( $icon_folder . "/procedure_32x32.png" );
    $self->{icons}->{PROCEDURE_COLLECTION}  = $self->{icons}->{PROCEDURE};
    
    # These are for the context-sensitive menus ...
    # TODO: how do we render the new-style icons ( FDO icon names )?
    $self->{icons}->{BAYESIAN}              = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-select-color'          , 'menu' ) );
    $self->{icons}->{EDIT}                  = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-edit'                  , 'menu' ) );
    $self->{icons}->{DROP}                  = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-delete'                , 'menu' ) );
    $self->{icons}->{REFRESH}               = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-refresh'               , 'menu' ) );
    $self->{icons}->{TRUNCATE}              = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-clear'                 , 'menu' ) );
    $self->{icons}->{INSERT}                = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-add'                   , 'menu' ) );
    $self->{icons}->{CLONE}                 = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-copy'                  , 'menu' ) );
    $self->{icons}->{ACTIVITY}              = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-info'                  , 'menu' ) );
    
    $self->{treeview} = $treeview;
    
    # Create a hash of connection names to DB types. We use this to determine what class of connection
    # object to construct, based on a connection name
    my $sth = $self->{globals}->{local_db}->prepare(
        "select ConnectionName, DatabaseType from connections"
    );
    
    $sth->execute();
    
    $self->{connection_to_db_type_map} = $sth->fetchall_hashref( "ConnectionName" );
    
    $self->build_connections;
    
    # The notebook that holds the SQL editors & results ...
    $self->{notebook} = Gtk3::Notebook->new();
    if ( $self->{globals}->{config_manager}->simpleGet( 'window::browser::vertical_tabs' ) == 1 ) {
        $self->{notebook}->set( 'tab-pos' , 'GTK_POS_LEFT' );
    }
    $self->{notebook}->set_scrollable( TRUE);
    $self->{builder}->get_object( 'main_paned' )->add2( $self->{notebook} );
    $self->{notebook}->show;
    
    # This mapping is used to determine which database we connect to initially, to list other databases
    $self->{db_type_roots} = {
        Netezza     => 'SYSTEM'
      , Oracle      => undef
    };
    
    my $pages = $self->{globals}->{local_db}->select(
        "select * from browser_pages order by PageIndex"
    );
    
    my $pages_count = 0;
    foreach my $page ( @{$pages} ) {
        if ( $self->load_page( $page->{Path} ) ) {
            my $this_page_objects = $self->{pages}->[ $pages_count ];
            $this_page_objects->{changelock} = 1;
            if ( $page->{RestoreConnection} ) {
                $this_page_objects->{target_chooser}->set_connection_name( $page->{ConnectionName} );
                $this_page_objects->{target_chooser}->set_database_name( $page->{DatabaseName} );
                $this_page_objects->{restore_connection_checkbox}->set_active( 1 );
            }
            if ( $page->{LockConnection} ) {
                $this_page_objects->{lock_connection_checkbox}->set_active( 1 );
            }
            $this_page_objects->{changelock} = 0;
            $pages_count ++;
        } else {
            $self->{globals}->{local_db}->do( "delete from browser_pages where Path = ?", [ $page->{Path} ] ) # maybe the file this page was point to is renamed / deleted
        }
    }
    
    if ( ! $pages_count ) {
        $self->add_blank_page;
    }
    
    $self->{treeview}->get_selection->signal_connect( changed  => sub { $self->on_tree_row_select( @_ ); } );
    
    $self->{mem_dbh}->do(
        "create table browser_columns( column_name text, column_type text, nullable integer )"
    );
    
    $self->{columns} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{mem_dbh}
      , read_only               => 1
      , column_sorting          => 1
      , sql                     => {
                                      select    => "column_name, column_type, nullable"
                                    , from      => "browser_columns"
                                   }
      , fields                  => [
                                        {
                                            name        => "column_name"
                                          , x_percent   => 50 
                                        }
                                      , {
                                            name        => "column_type"
                                          , x_percent   => 50
                                        }
                                      , {
                                            name        => "nullable"
                                          , x_absolute  => 80
                                          , renderer    => "toggle"
                                        }
                                   ]
      , vbox                    => $self->{builder}->get_object( "fields_datasheet_box" )
    } );
    
    $self->{columns}->{treeview}->signal_connect(
        button_press_event => sub { $self->on_columns_datasheet_click( @_ ) }
    );
    
    $self->{builder}->get_object( 'LeftHandTree' )->set_position( $self->{globals}->{config_manager}->simpleGet( 'window::browser:LeftHandTree:position' ) || 400 );
    
    return $self;
    
}

sub on_columns_datasheet_click {
    
    my ( $self, $widget, $event ) = @_;
    
    if ( $event->type ne '2button-press' ) {
        return;
    }
    
    my $column = $self->{columns}->get_column_value( "column_name" );
    
    my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE ) || return;
    my $database        = $self->get_selected_by_object_type( DATABASE_TYPE );
    my $schema          = $self->get_selected_by_object_type( SCHEMA_TYPE );
    my $connection      = $self->get_db_connection( $connection_name, $database );
    my $table           = $self->get_selected_by_object_type( TABLE_TYPE );
    
    my $alias           = $self->get_object_alias( $connection_name, $database, $schema, $table );
    
    #my $qualified_column_string = $connection->db_schema_table_string( $database, $schema, $table ) . '.' . $column;
    
    my $qualified_column_string = $alias . '.' . $column
      . ( ' ' x ( COLUMN_EXPRESSION_LENGTH - length( $alias . '.' . $column ) ) )
      . " as " . $column;
    
    my $page_index = $self->{notebook}->get_current_page;
    
    my $treeview = $self->{pages}->[ $page_index ]->{sql_editor};
    my $buffer = $treeview->get_buffer;
    
    $buffer->insert_at_cursor( ", " . $qualified_column_string );
    $treeview->grab_focus;
    
}

sub on_tree_row_select {
    
    my ( $self, $tree_selection ) = @_;
    
    my ( $model, $iters ) = $self->get_selected_iters;
    
    foreach my $iter ( @{$iters} ) {
        
        my $type = $model->get( $iter, TYPE_COLUMN );

        if ( $type eq TABLE_TYPE ) {

            my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE ) || return;
            my $database        = $self->get_selected_by_object_type( DATABASE_TYPE );
            my $connection      = $self->get_db_connection( $connection_name, $database );
            my $schema          = $self->get_selected_by_object_type( SCHEMA_TYPE );
            my $table           = $self->get_selected_by_object_type( TABLE_TYPE );

            my $class = ref $connection;

            $self->populate_columns_datasheet( $connection, $database, $schema, $table );

        } elsif ( $type eq VIEW_TYPE ) {

            my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE ) || return;
            my $database        = $self->get_selected_by_object_type( DATABASE_TYPE );
            my $connection      = $self->get_db_connection( $connection_name, $database );
            my $schema          = $self->get_selected_by_object_type( SCHEMA_TYPE );
            my $view            = $self->get_selected_by_object_type( VIEW_TYPE );

            my $class = ref $connection;

            $self->populate_columns_datasheet( $connection, $database, $schema, $view );

        }
        
    }
    
}

sub populate_columns_datasheet {
    
    my ( $self, $connection, $database, $schema, $table ) = @_;
    
    my $columns    = $connection->fetch_column_info( $database, $schema, $table );
    my $field_list = $connection->fetch_field_list( $database, $schema, $table );
    
    my $upper_case_columns_hash;
    
    # TODO: this is horrible, but I don't have time to deal with it now ...
    foreach my $column_name ( keys %{$columns} ) {
        $upper_case_columns_hash->{ uc($column_name) } = $columns->{$column_name}
    }
    
    $self->{mem_dbh}->do( "delete from browser_columns" );
    
    my $sth = $self->{mem_dbh}->prepare(
        "insert into browser_columns ( column_name, column_type, nullable ) values ( ?, ?, ? )"
    );
    
    foreach my $column_name ( @{$field_list} ) {
        
        my $this_column_info = $upper_case_columns_hash->{ uc( $column_name ) };
        
        no warnings 'uninitialized';
        
        $sth->execute(
            $this_column_info->{COLUMN_NAME}
          , $this_column_info->{DATA_TYPE} . $this_column_info->{PRECISION}
          , $this_column_info->{NULLABLE}
        );
        
    }
    
    $self->{columns}->query;
    
}

sub on_page_close_clicked {

    my ( $self ) = @_;
    
    my $page_index = $self->{notebook}->get_current_page();
    
    print "Closing page [$page_index]\n";
    
    $self->{notebook}->remove_page( $page_index );
    splice( @{ $self->{pages} }, $page_index, 1 );
    
    my $local_db = $self->{globals}->{local_db};
    
    $local_db->do( "delete from browser_pages where PageIndex = ?", [ $page_index ] );
    
    # TODO: this doesn't appear to work all the time - we occasionally get gaps in our PageIndex, which puts us out of whack with the GtkNotebook page indexes
    $local_db->do( "update browser_pages set PageIndex = PageIndex - 1 where PageIndex > ?", [ $page_index ] );
    
}

sub save_current_page {
    
    my ( $self , $null_page ) = @_;
    
    my $page_index = $self->{notebook}->get_current_page;
    
    $self->save_page( $page_index , $null_page );
    
}

sub load_page_from_file {
    
    my $self = shift;
    
    my $path = $self->file_chooser(
        {
            title       => 'Locate a file to open'
          , type        => 'file'
          , path        => $self->{globals}->{config_manager}->simpleGet( 'window::browser:last_opened_folder' )
        }
    );
    
    if ( $path ) {
        $self->load_page( $path );
        my $page_index = $self->{notebook}->get_current_page;
        $self->{globals}->{local_db}->do( "insert into browser_pages( PageIndex, Path ) values ( ?, ? )", [ $page_index, $path ] );
        my ( $filename, $directory, $suffix ) = fileparse( $path );
        $self->{globals}->{config_manager}->simpleSet( 'window::browser:last_opened_folder', $directory );
    }
    
}

sub load_page {
    
    my ( $self, $path ) = @_;
    
    my $sql;

    if ( $path ) {

        eval {

            open TARGET_FILE , "<" . $path
                or die( "Failed to open target file for reading:\n" . $! );

            while ( <TARGET_FILE> ) {
                $sql .= $_;
            }

            close TARGET_FILE
                or die( "Failed to close target file:\n" . $! );

        };

        my $err = $@;

        if ( $err ) {

            $self->dialog(
                {
                    title  => "Error loading from file [$path]"
                  , type => "error"
                  , text => $err
                }
            );

            return FALSE;

        }

    }
    
    my @path_bits = split( /(\\|\/)/, $path );
    my $page_name = $path_bits[$#path_bits];
    
    $self->add_page( $page_name, $sql, $path );
    
    return TRUE;
    
}

sub save_page {
    
    my ( $self, $page_index, $null_path ) = @_;
    
    my $sql                 = $self->get_buffer_value( $self->{pages}->[ $page_index ]->{sql_editor} );
    my $page_details        = $self->{pages}->[ $page_index ];
    my $page_name           = $page_details->{page_label}->get_text;
    my $connection_name     = $page_details->{target_chooser}->get_connection_name;
    my $database_name       = $page_details->{target_chooser}->get_database_name;
    my $restore_connection  = $page_details->{restore_connection_checkbox}->get_active;
    my $lock_connection     = $page_details->{lock_connection_checkbox}->get_active;

    my $new_page = 0;
    
    if ( ! $page_details->{path} && ! $null_path ) {
        
        $page_details->{path} = $self->file_chooser(
            {
                title   => "Please choose a target file for [$page_name]"
              , action  => "save"
              , type    => "file"
              , path    => $self->{globals}->{config_manager}->simpleGet( 'window::browser:last_saved_folder' )
            }
        );
        
        $new_page = 1;
        
    }
    
    if ( ! $page_details->{path} && ! $null_path ) {
        return;
    }
    
    if ( $new_page || $null_path ) {
        my ( $filename, $directory, $suffix, $local_page_name );
        if ( ! $null_path ) {
            ( $filename, $directory, $suffix ) = fileparse( $page_details->{path} );
            $page_name = $filename . $suffix;
        }
        $page_details->{page_label}->set_text( $local_page_name || $page_name || '' );
        if ( ! $null_path ) {
            $self->{globals}->{config_manager}->simpleSet('window::browser:last_saved_folder' , $directory);
        }
    }

    if ( ! $null_path ) {

        eval {

            open TARGET_FILE , ">" . $page_details->{path}
                || die( "Failed to open target file for writing:\n" . $!) ;

            print TARGET_FILE $sql;

            close TARGET_FILE
                || die( "Failed to close target file:\n" . $! );

        };

    }
    
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
    
    my $local_db = $self->{globals}->{local_db};
    
    $local_db->do( "delete from browser_pages where PageIndex = ?", [ $page_index ] );
    
    $local_db->do( "insert into browser_pages( PageIndex, Path, ConnectionName, DatabaseName, RestoreConnection, LockConnection ) values ( ?, ?, ?, ?, ?, ? )"
                 , [ $page_index, $page_details->{path}, $connection_name, $database_name, $restore_connection, $lock_connection ]
    );
    
    $page_details->{changed} = 0;
    
    $page_details->{page_label}->set_markup( "<span color='blue'>" . $page_details->{page_label}->get_text . "</span>" );
    
}

sub add_blank_page {

    my $self = shift;

    $self->add_page;
    $self->set_current_page_active_connection;
    $self->set_current_page_active_database;
    $self->save_current_page ( 1 );

}

sub add_page {
    
    my ( $self, $page_name, $sql, $path ) = @_;
    
    # Build the contents of the page
    
    ############################################################
    
    my $paned               = Gtk3::Paned->new( 'GTK_ORIENTATION_VERTICAL' );
    my $vbox                = Gtk3::Box->new( 'GTK_ORIENTATION_VERTICAL' , 5 );
    my $sw                  = Gtk3::ScrolledWindow->new();
    
    $paned->set_wide_handle( TRUE );
    
    # Connection & DB chooser
    my $target_chooser_box  = Gtk3::Box->new( 'GTK_ORIENTATION_HORIZONTAL', 5 );
    my $target_chooser      = widget::conn_db_table_chooser->new(
          $self->{globals}
        , $target_chooser_box
        , {
              database    => 1
            , schema      => 0
            , table       => 0
          }
        , {
              on_connection_changed         => sub { $self->on_sql_editor_changed } # triggers the status change in page label
            , on_database_changed           => sub { $self->on_sql_editor_changed } # triggers the status change in page label
          }
    );
    $target_chooser_box->set_homogeneous( TRUE );

    # Checkboxes for 'restore on startup' and 'lock connection'
    my $target_options_box          = Gtk3::Box->new( 'GTK_ORIENTATION_VERTICAL', 5 );
    my $restore_connection_checkbox = Gtk3::CheckButton->new( 'Restore on Startup' );
    $restore_connection_checkbox->signal_connect_after( 'toggled', sub { $self->on_sql_editor_changed } );

    my $lock_connection_checkbox    = Gtk3::CheckButton->new( 'Lock Connection' );
    $lock_connection_checkbox->signal_connect_after( 'toggled', sub { $self->on_sql_editor_changed } );

    $target_options_box->pack_start( $restore_connection_checkbox, TRUE, TRUE, 2 );
    $target_options_box->pack_start( $lock_connection_checkbox, TRUE, TRUE, 2 );

    my $target_box                  = Gtk3::Box->new( 'GTK_ORIENTATION_HORIZONTAL', 5 );
    $target_box->pack_start( $target_chooser_box, TRUE, TRUE, 5 );
    $target_box->pack_start( $target_options_box, FALSE, FALSE, 5 );

    # Create the GtkSourceView widget and set up syntax highlighting
    my $sql_editor          = Gtk3::SourceView::View->new();
    
    my $view_buffer         = Gtk3::SourceView::Buffer->new_with_language( $self->{globals}->{gtksourceview_language} );
    $view_buffer->set_highlight_syntax( TRUE );
    $view_buffer->set_style_scheme( $self->{globals}->{gtksourceview_scheme} );
    $sql_editor->set_buffer( $view_buffer );
    
    if ( &Gtk3::MINOR_VERSION >= 16 ) {
        $sql_editor->set_background_pattern( 'GTK_SOURCE_BACKGROUND_PATTERN_TYPE_GRID' );
    }
    
    $sql_editor->set_tab_width( 4 );
    
    # The action buttons
    my $execute_button              = $self->image_button( "Execute SQL ...",       'gtk-execute' );
    my $hash_expression             = $self->image_button( "Hash expression ...",   'gtk-about' );
    my $undo_button                 = $self->image_button( "Undo",                  'gtk-undo' );
    my $redo_button                 = $self->image_button( "Redo",                  'gtk-redo' );
    my $start_autorefresh_button    = $self->image_button( "Start Autorefresh ...", 'gtk-media-play' );
    my $stop_autorefresh_button     = $self->image_button( "Stop Autorefresh ...",  'gtk-media-stop' );
    my $execute_direct_button       = $self->image_button( "Execute Direct\n( no prepare ) ...",  'gtk-execute' );

    # Up and Down pane buttons
    my $pane_up_button      = Gtk3::Button->new;
    my $pane_up_icon        = Gtk3::Image->new_from_icon_name( 'gtk-go-up', 'button' );
    $pane_up_button->set_image( $pane_up_icon );
    $pane_up_button->set( 'always-show-image', TRUE );
    $pane_up_button->set_tooltip_markup( 'Move pane up' );
    $pane_up_button->signal_connect( 'button-press-event', sub { $self->move_pane_up( @_ ) } );
    
    my $pane_down_button    = Gtk3::Button->new;
    my $pane_down_icon      = Gtk3::Image->new_from_icon_name( 'gtk-go-down', 'button' );
    $pane_down_button->set_image( $pane_down_icon );
    $pane_down_button->set( 'always-show-image', TRUE );
    $pane_down_button->set_tooltip_markup( 'Move pane down' );
    $pane_down_button->signal_connect( 'button-press-event', sub { $self->move_pane_down( @_ ) } );
    
    my $hbox                = Gtk3::Box->new( 'GTK_ORIENTATION_HORIZONTAL' , 2 );
    
    $hbox->pack_start( $pane_up_button, FALSE, TRUE, 2 );
    # $hbox->pack_start( $hash_expression, TRUE, TRUE, 2 );
    $hbox->pack_start( $undo_button, TRUE, TRUE, 2 );
    $hbox->pack_start( $redo_button, TRUE, TRUE, 2 );
    $hbox->pack_start( $execute_button, TRUE, TRUE, 2 );
    $hbox->pack_start( $execute_direct_button, TRUE, TRUE, 2 );
    $hbox->pack_start( $start_autorefresh_button, TRUE, TRUE, 2 );
    $hbox->pack_start( $stop_autorefresh_button, TRUE, TRUE, 2 );
    $hbox->pack_start( $pane_down_button, FALSE, TRUE, 2 );
    
    #  ... and also some tags for syntax highlighting
    #$self->setup_tags( $sql_editor );
    
    #  ... and a border
    foreach my $border_type ( qw ! GTK_TEXT_WINDOW_LEFT GTK_TEXT_WINDOW_RIGHT GTK_TEXT_WINDOW_TOP GTK_TEXT_WINDOW_BOTTOM ! ) {
        $sql_editor->set_border_window_size( $border_type , 2 );
    }
    
    #  ... and text wrapping
    $sql_editor->set_wrap_mode( 'GTK_WRAP_WORD' );
    
    #  ... the box that we create the results viewer in
    my $results_viewer_box = Gtk3::Box->new( 'GTK_ORIENTATION_VERTICAL' , 2 );
    
    # Now put everything together
    $sw->add( $sql_editor );
    $vbox->pack_start( $target_box, FALSE, TRUE, 2  );
    $vbox->pack_start( $sw, TRUE, TRUE, 2 );
    $vbox->pack_start( $hbox, FALSE, TRUE, 2 );
    $paned->set_position( $self->{globals}->{config_manager}->simpleGet( 'browser::vertical_pane' ) || 400 );
    $paned->add1( $vbox );
    $paned->add2( $results_viewer_box );
    
    # The label for the page
    my $page_label = Gtk3::Label->new();
    
    # The close button for the page
    my $close_button = Gtk3::Button->new();
    
    # We need to keep rendering new images apparently, or the button loses the image when we add another page. Not sure why
    my $close_image = Gtk3::Image->new_from_pixbuf( $self->{builder}->get_object( 'browser' )->render_icon( 'gtk-delete', 'menu' ) );
    $close_button->set_image( $close_image );
    
    # A box to hold the label and close button
    my $box = Gtk3::Box->new( 'GTK_ORIENTATION_HORIZONTAL', 1 );
    $box->pack_start( $page_label, TRUE, TRUE, 1 );
    $box->pack_start( $close_button, TRUE, TRUE, 1 );
    
    $box->show_all;
    
    # Finally create the new page and pass the contents into it ...
    my $page_index = $self->{notebook}->append_page( $paned , $box );
    
    $close_button->signal_connect( 'clicked' => sub { $self->on_page_close_clicked( $page_index ) } );
    
    # Now that we know what page we've just created, shove things into the 'pages' array
    # and hook up the 'execute query' button ...
    $self->{pages}->[ $page_index ] = {
        results_viewer_box              => $results_viewer_box
      , target_chooser                  => $target_chooser
      , restore_connection_checkbox     => $restore_connection_checkbox
      , lock_connection_checkbox        => $lock_connection_checkbox
      , sql_editor                      => $sql_editor
      , page_label                      => $page_label
      , label_box                       => $box
      , paned                           => $paned
      , path                            => $path
    };
    
    $self->{new_page_counter} ++; # independent of the $page_index, as people can remove pages
    
    if ( $sql ) {
        $sql_editor->get_buffer->set_text( $sql );
    }
    
    if ( $page_name ) {
        $page_label->set_markup( "<span color='blue'>" . $self->escape( $page_name ) . "</span>" );
    } else {
        $page_label->set_markup( "<span color='blue'>Page " . $self->{new_page_counter}. "</span>" );
    }
    
    $paned->show_all;
    
    $self->{notebook}->set_current_page( $page_index );
    
    $execute_button->signal_connect_after(   'clicked'  => sub { $self->on_execute_clicked() } );
    $execute_direct_button->signal_connect_after(   'clicked'  => sub { $self->on_execute_clicked( undef , TRUE ) } );
    $hash_expression->signal_connect_after( 'clicked'   => sub { $self->on_hash_expression_clicked() } );
    $undo_button->signal_connect_after( 'clicked'       => sub { $self->on_undo_clicked() } );
    $redo_button->signal_connect_after( 'clicked'       => sub { $self->on_redo_clicked() } );

    $start_autorefresh_button->signal_connect_after( 'clicked'  => sub { $self->StartAutoRefresh() } );
    $stop_autorefresh_button->signal_connect_after( 'clicked'  => sub { $self->StopAutoRefresh() } );

    $sql_editor->get_buffer->signal_connect( 'changed'  => sub { $self->on_sql_editor_changed() } );
    
    $sql_editor->signal_connect( 'key_press_event'      => sub { $self->on_sourceview_key_press_event( @_ ) } );
    
}

sub on_hash_expression_clicked {
    
    my $self = shift;
    
    my $page_index = $self->{notebook}->get_current_page;
    my $buffer = $self->{pages}->[ $page_index ]->{sql_editor}->get_buffer;
    
    my ( $start_iter , $end_iter ) = $buffer->get_selection_bounds;
    
    if ( ! $start_iter || ! $end_iter ) {
        return;
    }
    
    my $expression = $buffer->get_text( $start_iter, $end_iter, 0 );
    
    my $hash_expression = 'encode( digest( ' . $expression . "::VARCHAR, 'sha256' ), 'base64' )";
    
    if ( $expression =~ /([\w]*)$/ ) {
        $hash_expression .= ' as ' . $1;
    }
    
    $buffer->delete( $start_iter, $end_iter );
    $buffer->insert( $start_iter, $hash_expression );
    
}

sub on_sql_editor_changed {
    
    my $self = shift;
    
    my $page_index = $self->{notebook}->get_current_page;
    
    my $page_def = $self->{pages}->[ $page_index ];

    if ( ! $page_def->{changed} && ! $page_def->{changelock} ) {
        $page_def->{changed} = 1;
        $page_def->{page_label}->set_markup( "<span color='red'>" . $page_def->{page_label}->get_text . "</span>" );
    }
    
}

sub move_pane_left {
    
    my $self = shift;
    
    my $horizontal_pane = $self->{builder}->get_object( 'main_paned' );
    my $new_position  = $horizontal_pane->get_position - 5;
    $horizontal_pane->set_position( $new_position );
    
    $self->{globals}->{config_manager}->simpleSet( 'browser::horizontal_pane', $new_position );
    
}

sub move_pane_right {
    
    my $self = shift;
    
    my $horizontal_pane = $self->{builder}->get_object( 'main_paned' );
    my $new_position  = $horizontal_pane->get_position + 5;
    $horizontal_pane->set_position( $new_position );
    
    $self->{globals}->{config_manager}->simpleSet( 'browser::horizontal_pane', $new_position );
    
}

sub move_pane_up {
    
    my $self = shift;
    
    my $page_index = $self->{notebook}->get_current_page;
    
    my $vertical_pane = $self->{pages}->[ $page_index ]->{paned};
    my $new_position  = $vertical_pane->get_position - 5;
    $vertical_pane->set_position( $new_position );
    
    $self->{globals}->{config_manager}->simpleSet( 'browser::vertical_pane', $new_position );
    
}

sub move_pane_down {
    
    my $self = shift;
    
    my $page_index = $self->{notebook}->get_current_page;
    
    my $vertical_pane = $self->{pages}->[ $page_index ]->{paned};
    my $new_position  = $vertical_pane->get_position + 5;
    $vertical_pane->set_position( $new_position );
    
    $self->{globals}->{config_manager}->simpleSet( 'browser::vertical_pane', $new_position );
    
}

sub setup_tags {
    
    # Not in use - replaced with Gtk3::SourceView
    
    my ( $self, $textview ) = @_;
    
    my $target = $textview->get_buffer;
    
    my $bold = Glib::Object::Introspection->convert_sv_to_enum( "Pango::Weight", "bold" );
    
    foreach my $colour ( "black", "blue", "green", "red", "purple", "darkgreen" ) {
        
        $target->create_tag(
            $colour
          , weight      => $bold
          , foreground  => $colour
        );
        
    }
    
    $target->signal_connect_after( changed => sub { $self->highlight_sql_regex( $target ) } );
    
}

sub highlight_sql_regex {
    
    # Not in use - replaced with Gtk3::SourceView
    
    my ( $self, $buffer ) = @_;
    
    my @highlight_list = (
        [    '(select|from|where|group\s*by|limit)'               , 'black'       ]
      , [    '\b[A-Za-z_0-9]+\.[A-Za-z_0-9]*\.[A-Za-z_0-9]+\b'    , 'blue'        ]
    );
    
    my ( $start_iter, $end_iter ) = $buffer->get_bounds;
    my $value = $buffer->get_text( $start_iter, $end_iter, 1 );
    
    for my $regex_highlighter ( @highlight_list ) {
        
        my ( $start_position, $end_position );
        
        my @match_positions = $self->match_all_positions( $$regex_highlighter[0], $value );
        
        foreach my $position_set ( @match_positions ) {
            my $this_start_iter = $buffer->get_iter_at_offset( $$position_set[0] );
            my $this_end_iter   = $buffer->get_iter_at_offset( $$position_set[1] );
            $buffer->apply_tag_by_name( $$regex_highlighter[1], $this_start_iter, $this_end_iter );
        }
        
    }
    
}

sub match_all_positions {
    
    # Not in use - replaced with Gtk3::SourceView
    
    my ( $self, $regex, $string ) = @_;
    
    my @ret;
    
    while ( $string =~ /$regex/img ) {
        push @ret, [ $-[0], $+[0] ];
    }
    
    return @ret
    
}

sub render_pixbuf_cell {
    
    my ( $self, $tree_column, $renderer, $model, $iter ) = @_;
    
    my $type = $model->get( $iter, TYPE_COLUMN );
    
    if ( $type eq CONNECTION_TYPE ) {
        
        my $db_type = $model->get( $iter, EXTRA_1_COLUMN );
        
        $renderer->set( pixbuf => $self->{icons}->{ $db_type } );
        
    } else {
        
        $renderer->set( pixbuf => $self->{icons}->{ $type } );
        
    }
    
    return FALSE;
    
}

sub get_db_connection {
    
    my ( $self, $connection_name, $database_name ) = @_;
    
    my $key = $connection_name . ':' . ( defined $database_name ? $database_name : '' );
    
    if ( ! exists $self->{connections}->{ $key } ) {
        
        my $auth_hash = $self->{globals}->{config_manager}->get_auth_values( $connection_name );

        # Forces rebuilding string - otherwise some DBs ( eg SQL Server / Synapse ) won't add the database to the connection string
        $auth_hash->{ConnectionString} = undef;

        if ( ! $auth_hash->{Database} ) {
            $auth_hash->{Database} = $database_name;
        }
        
        $self->{connections}->{ $key } = Database::Connection::generate(
            $self->{globals}
          , $auth_hash
          , undef
          , undef
          , undef
          , {
                dont_force_case    => 1
            }
        );
        
        # Disable named placeholders - we don't use them here,
        # and they mess with things ...
        $self->{connections}->{ $key }->{connection}->{odbc_ignore_named_placeholders} = 1;
        
    }
    
    return $self->{connections}->{ $key };
    
}

sub remove_node {
    
    my ( $self, $node_iter ) = @_;
    
    while ( my $iter = $self->{model}->iter_children( $node_iter ) ) {
        $self->remove_node( $iter );
    }
    
    $self->{model}->remove( $node_iter );
    
}

sub remove_children {
    
    my ( $self, $node_iter ) = @_;
    
    while ( my $iter = $self->{model}->iter_children( $node_iter ) ) {
        $self->remove_node( $iter );
    }
    
}

sub build_connections {
    
    my $self = shift;
    
    my $model = $self->{group_hierarchy_model} = Gtk3::TreeStore->new(
        qw' Glib::String Glib::String Glib::String Glib::String Glib::String Glib::String '
    );
    
    my $connections = $self->{globals}->{local_db}->select(
        "select    ConnectionName, DatabaseType\n"
      . "from      connections\n"
      . "order by  ConnectionName"
    );
    
    if ( ! $connections ) {
        
        $self->dialog(
            {
                title       => "Please add a connection"
              , type        => "info"
              , text        => "There are no connections configured yet. The configuration screen will now be opened."
                             . " After you've added a connection, click the 'refresh' button, here, to list available connections" 
            }
        );
        
        $self->open_window( 'window::configuration', $self->{globals} );
        
        return;
        
    }
    
    foreach my $connection ( @{$connections} ) {
        
        my $connection_name = $connection->{ConnectionName};
        
        my $db_type = $self->{connection_to_db_type_map}->{ $connection_name }->{DatabaseType};
        
        if ( ! $self->{icons}->{ $db_type } ) {
            my $icon_path = $self->get_db_icon_path( $db_type . '.png' );
            if ( $icon_path && -e $icon_path ) {
                print "Rendering image for [$icon_path]\n";
                $self->{icons}->{ $db_type } = $self->to_pixbuf( $icon_path );
            } else {
                print "Using cached image for [$icon_path]\n";
            }
        }
        
        $model->set(
            $model->append
          , TYPE_COLUMN,        CONNECTION_TYPE
          , OBJECT_COLUMN,      $connection->{ConnectionName}
          , EXTRA_1_COLUMN,     $db_type
        );
        
    }
    
    $self->{builder}->get_object( 'browser_tree' )->set_model( $model );
    
    $self->{model} = $model;
    
}

sub build_databases {
    
    my ( $self, $iter, $connection_name ) = @_;
    
    $self->remove_children( $iter );
    
    my $db_type = $self->{connection_to_db_type_map}->{ $connection_name }->{DatabaseType};
    my $root_database = $self->{db_type_roots}->{ $db_type };
    
    my $connection = $self->get_db_connection(
        $connection_name
      , $root_database
    ) || return;
    
    my @databases = $connection->fetch_database_list();
    
    foreach my $database ( @databases ) {
        
        my $database_iter = $self->{model}->append( $iter );
        
        $self->{model}->set(
            $database_iter
          , TYPE_COLUMN,        DATABASE_TYPE
          , OBJECT_COLUMN,      $database
        );
        
    }
    
    my ( $model, $selected_path, $this_iter, $type, $object_name ) = $self->get_clicked_tree_object( $self->{treeview} );
    
    $self->{treeview}->expand_to_path( $selected_path );
    
}

sub build_all_database_objects {
    
    my ( $self, $database_iter, $database ) = @_;
    
    my $connection_name = $self->{model}->get( 
        $self->{model}->iter_parent( $database_iter )
      , OBJECT_COLUMN
    );
    
    my $this_dbh = $self->get_db_connection( $connection_name, $database );
    
    $self->remove_children( $database_iter );
    
    if ( $this_dbh->has_schemas ) {
        
        my $schema_type_iter = $self->{model}->append( $database_iter );
        
        $self->{model}->set(
            $schema_type_iter
          , TYPE_COLUMN,    SCHEMA_COLLECTION_TYPE
          , OBJECT_COLUMN,  "schemas"
        );
        
        my @schemas = $this_dbh->fetch_schema_list( $database );
        
        my $schema_count = @schemas;
        $self->{pulse_amount}   = 1 / $schema_count;
        
        foreach my $schema ( @schemas ) {
            
            $self->pulse( "Fetching schema [$schema]" );
            
            no warnings 'uninitialized';
            
            my $this_schema_iter = $self->{model}->append( $schema_type_iter );
            
            $self->{model}->set(
                $this_schema_iter
              , TYPE_COLUMN,    SCHEMA_TYPE
              , OBJECT_COLUMN,  $schema
            );
            
            $self->build_db_objects_tree( $this_dbh, $database, $schema, $this_schema_iter );
            
        }
    
        $self->reset_progress;
        
    } else {
        
        $self->build_db_objects_tree( $this_dbh, $database, undef, $database_iter );
        
    }
    
}

sub build_db_objects_tree {
    
    my ( $self, $this_dbh, $database, $schema, $parent_iter ) = @_;
    
    ####################
    # tables
    
    my $table_type_iter = $self->{model}->append( $parent_iter );
    
    $self->{model}->set(
        $table_type_iter
      , TYPE_COLUMN,    TABLE_COLLECTION_TYPE
      , OBJECT_COLUMN,  "tables"
    );
    
    my @tables = $this_dbh->fetch_table_list( $database, $schema );
    
    foreach my $table ( @tables ) {
        
        no warnings 'uninitialized';
        
        my $this_table_iter = $self->{model}->append( $table_type_iter );
        
        $self->{model}->set(
            $this_table_iter
          , TYPE_COLUMN,    TABLE_TYPE
          , OBJECT_COLUMN,  $table
        );
        
    }
    
    my $view_type_iter = $self->{model}->append( $parent_iter );
    
    $self->{model}->set(
        $view_type_iter
      , TYPE_COLUMN,        VIEW_COLLECTION_TYPE
      , OBJECT_COLUMN,      "views"
    );
    
    my @views = $this_dbh->fetch_view_list( $database, $schema );
    
    foreach my $view ( @views ) {
        
        $self->{model}->set(
            $self->{model}->append( $view_type_iter )
          , TYPE_COLUMN,    VIEW_TYPE
          , OBJECT_COLUMN,  $view
        );
        
    }

    my $mview_type_iter = $self->{model}->append( $parent_iter );

    $self->{model}->set(
        $mview_type_iter
      , TYPE_COLUMN,        MVIEW_COLLECTION_TYPE
      , OBJECT_COLUMN,      "materialized views"
    );

    my @mviews = $this_dbh->fetch_materialized_view_list( $database, $schema );

    foreach my $mview ( @mviews ) {

        $self->{model}->set(
            $self->{model}->append( $mview_type_iter )
          , TYPE_COLUMN,    MVIEW_TYPE
          , OBJECT_COLUMN,  $mview
        );

    }

    my $function_type_iter = $self->{model}->append( $parent_iter );
    
    $self->{model}->set(
        $function_type_iter
      , TYPE_COLUMN,        FUNCTION_COLLECTION_TYPE
      , OBJECT_COLUMN,      "functions"
    );
    
    my @functions = $this_dbh->fetch_function_list( $database, $schema );
    
    foreach my $function ( @functions ) {
        
        $self->{model}->set(
            $self->{model}->append( $function_type_iter )
          , TYPE_COLUMN,    FUNCTION_TYPE
          , OBJECT_COLUMN,  $function
        );
        
    }
    
    my $procedure_type_iter = $self->{model}->append( $parent_iter );
    
    $self->{model}->set(
        $procedure_type_iter
      , TYPE_COLUMN,        PROCEDURE_COLLECTION_TYPE
      , OBJECT_COLUMN,      "procedures"
    );
    
    my @procedures = $this_dbh->fetch_procedure_list( $database, $schema );
    
    foreach my $procedure ( @procedures ) {
        
        $self->{model}->set(
            $self->{model}->append( $procedure_type_iter )
          , TYPE_COLUMN,    PROCEDURE_TYPE
          , OBJECT_COLUMN,  $procedure
        );
        
    }
    
}

sub on_tree_click {
    
    my ( $self, $widget, $event ) = @_;
    
    # The handlers for click events is split up to simplify things ...
    
    my $type    = $event->type;
    my $button  = $event->button;
    
    if ( $button == 3 ) {
        
        $self->build_context_menu( $widget, $event );
        
    } elsif ( $type eq '2button-press' ) {
        
        $self->handle_double_click( $widget, $event );
        
    }
    
}

sub build_context_menu {
    
    my ( $self, $widget, $event ) = @_;
    
    my ( $model, $selected_path, $iter, $type, $object_name ) = $self->get_clicked_tree_object( $widget );
    
    $self->{context_menu} = Gtk3::Menu->new;
    
    my $menu_control = {
        EDIT        => {
                            text        => 'edit object'
                          , method      => 'edit_object'
                          , objects     => [ DATABASE_TYPE , TABLE_TYPE , VIEW_TYPE , FUNCTION_TYPE , PROCEDURE_TYPE ]
        }
      , REFRESH     => {
                            text        => 'refresh objects'
                          , method      => 'refresh_objects'
                          , objects     => [ DATABASE_TYPE , TABLE_TYPE , VIEW_TYPE , FUNCTION_TYPE , PROCEDURE_TYPE ]
        }
      , DROP        => {
                            text        => 'drop object'
                          , method      => 'drop_object'
                          , objects     => [ DATABASE_TYPE , SCHEMA_TYPE , TABLE_TYPE , VIEW_TYPE , FUNCTION_TYPE , PROCEDURE_TYPE ]
        }
      , TRUNCATE    => {
                            text        => 'truncate table'
                          , method      => 'truncate_table'
                          , objects     => [ TABLE_TYPE ]
        }
      # , BAYESIAN    => {
      #                       text        => 'bayesian classifier'
      #                     , method      => 'bayesian_classification'
      #                     , objects     => [ TABLE_TYPE ]
      #   }
      , INSERT      => {
                            text        => 'insert statement'
                          , method      => 'insert_statement'
                          , objects     => [ TABLE_TYPE ]
        }
      , CLONE       => {
                            text        => 'clone schema'
                          , method      => 'clone_schema'
                          , objects     => [ SCHEMA_TYPE, TABLE_TYPE ]
        }
      , ACTIVITY    => {
                            text        => 'show activity'
                          , method      => 'show_activity'
                          , objects     => [ CONNECTION_TYPE , DATABASE_TYPE , SCHEMA_TYPE , TABLE_TYPE , VIEW_TYPE,  FUNCTION_TYPE , PROCEDURE_TYPE ]
        }
      # , DATA_EDIT   => {
      #                       text        => 'edit data'
      #                     , method      => 'edit_data'
      #                     , objects     => [ TABLE_TYPE ]
      #   }
      , REFRESH_MVIEW => {
                            text        => 'refresh materialized view'
                          , method      => 'refresh_materialized_view'
                          , objects     => [ MVIEW_TYPE ]
        }
    };
    
    foreach my $key ( sort keys %{$menu_control} ) {
        
        my $this_menu_control = $menu_control->{$key};
        
        if ( grep { $_ eq $type } @{ $this_menu_control->{objects} } ) {
            
            my $item = Gtk3::ImageMenuItem->new_with_label( $this_menu_control->{text} );
            
            $item->set_image( $self->{icons}->{$key} );
            $item->set_always_show_image( TRUE );
            
            my $method_name = $this_menu_control->{method};
            $item->signal_connect_after( activate  => sub { $self->$method_name() } );
            $self->{context_menu}->append( $item );
            
            $item->show;
            
        }
        
    }
    
    $self->{context_menu}->popup( undef, undef, undef, undef, $event->button, $event->time );
    
    $self->{context_menu}->show;
    
    return FALSE;
    
}

sub set_current_page_active_connection {

    my $self = shift;

    my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE ) || return;
    my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );
    my $database_name   = $self->get_selected_by_object_type( DATABASE_TYPE ) || '';
    my $page_index      = $self->{notebook}->get_current_page;
    my $target_chooser  = $self->{pages}->[ $page_index ]->{target_chooser};

    if ( ! $self->{pages}->[ $page_index ]->{lock_connection_checkbox}->get_active ) {
        $target_chooser->set_connection_name($connection_name);
    }

}

sub set_current_page_active_database {

    my $self = shift;

    my $database_name   = $self->get_selected_by_object_type( DATABASE_TYPE ) || '';
    my $page_index      = $self->{notebook}->get_current_page;
    my $target_chooser  = $self->{pages}->[ $page_index ]->{target_chooser};

    if ( ! $self->{pages}->[ $page_index ]->{lock_connection_checkbox}->get_active ) {
        $target_chooser->set_database_name($database_name);
    }

}

sub handle_double_click {
    
    my ( $self, $widget, $event ) = @_;
    
    my ( $model, $selected_path, $iter, $type, $object_name ) = $self->get_clicked_tree_object( $widget );
    
    if ( $type eq CONNECTION_TYPE ) {
        
        $self->build_databases( $iter, $object_name );
        $self->set_current_page_active_connection;
        
    } elsif ( $type eq DATABASE_TYPE ) {
        
        $self->build_all_database_objects( $iter, $object_name );
        $self->{treeview}->expand_to_path( $selected_path );
        $self->set_current_page_active_database;
        
    } elsif ( $type eq TABLE_TYPE || $type eq VIEW_TYPE || $type eq MVIEW_TYPE || $type eq COLUMN_TYPE ) {
        
        my $database = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema   = $self->get_selected_by_object_type( SCHEMA_TYPE );
        
        my $page_index = $self->{notebook}->get_current_page;
        
        my $buffer = $self->{pages}->[ $page_index ]->{sql_editor}->get_buffer;
        
        my $text = $self->get_buffer_value( $self->{pages}->[ $page_index ]->{sql_editor} );
        
        print "text: [$text]\n";
        
        my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE ) || return;
        my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );
        
        if ( $type eq TABLE_TYPE || $type eq VIEW_TYPE || $type eq MVIEW_TYPE ) {
            
            if ( $text ) {
                
                $buffer->insert_at_cursor( $connection->db_schema_table_string( $database, $schema, $object_name ) );
                
            } else {
                
                my $sql;
                
                if ( $connection->can( 'build_entire_select_query_string' ) ) {
                    
                    $sql = $connection->build_entire_select_query_string( $database, $schema, $object_name );
                    
                } else {
                    
                    my $columns_results = $self->{mem_dbh}->select( "select column_name from browser_columns" );
                    
                    my $alias = $self->get_object_alias(
                        $connection_name
                      , $database
                      , $schema
                      , $object_name
                    );
                    
                    my @columns;

                    foreach my $column_result ( @{$columns_results} ) {

                        if ( $connection->can_alias ) {

                            push @columns, $alias
                                . "."
                                . $column_result->{column_name}
                                . ( ' ' x ( COLUMN_EXPRESSION_LENGTH - length( $alias . "." . $column_result->{column_name} ) ) )
                                . " as " . $column_result->{column_name};

                        } else {

                            push @columns, $alias
                                . "."
                                . $column_result->{column_name};

                        }

                    }
                    
                    $sql = "select\n    "
                        . join( "\n  , ", @columns )
                        . "\nfrom\n"
                        . "    " . $connection->object_alias_string(
                                       $connection->db_schema_table_string( $database, $schema, $object_name )
                                     , $alias
                                   ) . "\n"
                        . $connection->limit_clause( 1000 );
                    
                    #$self->{formatter}->query( $sql );
                    #$sql = $self->{formatter}->beautify;
                    
                    $sql =~ s/\[\s/\[/g; # convert "[ " to "["
                    $sql =~ s/\s\]/\]/g; # convert " ]" to "]"
                    
                }
                
                $buffer->set_text( $sql );
                
            }
            
        }
        
    }
    
}

sub get_object_alias {
    
    my ( $self, $connection_name, $database_name, $schema_name, $object_name ) = @_;
    
    my $page_index      = $self->{notebook}->get_current_page;
    
    my ( $alias, $qualified_object );
    
    {
        no warnings 'uninitialized';
        $qualified_object = $connection_name . "." . $database_name . "." . $schema_name . "." . $object_name;
    }
    
    if ( ! exists $self->{pages}->[ $page_index ]->{alias_map}->{$qualified_object} ) {
        
        $alias = ( $self->dialog(
            {
                title       => "What alias would you like to use for this object?"
              , type        => "input"
              , default     => $object_name
              , geometry    => { x => 600 , y => 100 }
            }
        ) ) || $object_name;
        
        $self->{pages}->[ $page_index ]->{alias_map}->{$qualified_object} = $alias;
        
    }
    
    return $self->{pages}->[ $page_index ]->{alias_map}->{$qualified_object};
    
}

sub clone_schema {
    
    my $self = shift;
    
    my ( $selected_paths, $model ) = $self->{treeview}->get_selection->get_selected_rows;
    
    foreach my $selected_path ( @{$selected_paths} ) {
        
        my $iter = $model->get_iter( $selected_path );
        
        my $object_name     = $model->get( $iter, OBJECT_COLUMN );
        my $object_type     = $model->get( $iter, TYPE_COLUMN );
        my $database_name   = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name     = $self->get_selected_by_object_type( SCHEMA_TYPE );
        my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE );
        my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );
        
        my $target_schema = $self->dialog(
            {
                title       => "Clone Schema"
              , type        => "input"
              , text        => "Enter a target schema"
            }
        ) || return;
        
        my $tables_collection_iter = $model->iter_children( $iter );
        my $no_of_tables = $model->iter_n_children( $tables_collection_iter );
        
        for my $child_no ( 0 .. $no_of_tables - 1 ) {
            
            my $table_iter = $model->iter_nth_child( $tables_collection_iter, $child_no );
            my $this_table = $model->get( $table_iter, OBJECT_COLUMN );
            
            my $sql = "create table "
                . $connection->db_schema_table_string( $database_name , $target_schema , $this_table )
                . "\nas\nselect * from "
                . $connection->db_schema_table_string( $database_name , $schema_name , $this_table )
                . "\nwhere 0=1";
            
            $connection->do( $sql );

        }
        
        my $selected_database_iter = $self->get_selected_iter_by_object_type( DATABASE_TYPE );
        
        $self->build_all_database_objects( $selected_database_iter, $database_name  );
        
        $self->{treeview}->expand_all;
        
    }
    
    $self->dialog(
        {
            title   => "Schema cloned"
          , type    => "info"
          , text    => "Schema successfully cloned"
        }
    );
    
}

sub edit_data {
    
    my $self = shift;
    
    my ( $selected_paths, $model ) = $self->{treeview}->get_selection->get_selected_rows;
    
    foreach my $selected_path ( @{$selected_paths} ) {
        
        my $iter = $model->get_iter( $selected_path );
        
        my $object_name     = $model->get( $iter, OBJECT_COLUMN );
        my $object_type     = $model->get( $iter, TYPE_COLUMN );
        my $database_name   = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name     = $self->get_selected_by_object_type( SCHEMA_TYPE );
        my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE );
        my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );
        
        my $qualified_table = $connection->db_schema_table_string( $database_name, $schema_name, $object_name );
        
        $qualified_table    =~ s/"//g;
        
        my $page_index = $self->{notebook}->get_current_page;
        
        $self->{pages}->[ $page_index ]->{datasheet}->{sql} = {
            select          => "*"
          , from            => $qualified_table
        };
        
        my $treeview = $self->{pages}->[ $page_index ]->{sql_editor};
        my $buffer = $treeview->get_buffer;
        
        $buffer->insert_at_cursor( 'select * from ' . $qualified_table . "\n\n-- DO NOT EDIT, OR THINGS WILL GO <crazy>" );
        
    }
    
}

sub show_activity {
    
    my $self = shift;
    
    my ( $selected_paths, $model ) = $self->{treeview}->get_selection->get_selected_rows;
    
    $self->add_blank_page;
    
    foreach my $selected_path ( @{$selected_paths} ) {
        
        my $iter = $model->get_iter( $selected_path );
        
        my $object_name     = $model->get( $iter, OBJECT_COLUMN );
        my $object_type     = $model->get( $iter, TYPE_COLUMN );
        my $database_name   = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name     = $self->get_selected_by_object_type( SCHEMA_TYPE );
        my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE );
        my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );
        
        my $sql = $connection->generate_current_activity_query;
        
        my $page_index = $self->{notebook}->get_current_page;
        
        my $treeview = $self->{pages}->[ $page_index ]->{sql_editor};
        my $buffer = $treeview->get_buffer;
        
        $buffer->insert_at_cursor( $sql );
        
        $self->{pages}->[ $page_index ]->{datasheet}->{fields} = [
            {
                name        => "username"
              , x_absolute  => 120
            }
          , {
                name        => "db"
              , x_absolute  => 120
            }
          , {
                name        => "host"
              , x_absolute  => 120
            }
          , {
                name        => "id"
              , x_absolute  => 120
            }
          , {
                name        => "state"
              , x_absolute  => 120
            }
          , {
                name        => "query"
              , x_percent   => 100
            }
        ];
        
        $self->{pages}->[ $page_index ]->{datasheet}->{recordset_extra_tools} = {
            explain_query   => {
                type        => 'button'
              , markup      => "<b><span color='blue'>explain query</span></b>"
              , icon_name   => 'dialog-information'
              , coderef     => sub { $self->activity_explain_query }
            }
          , cancel_query    => {
                type        => 'button'
              , markup      => "<b>cancel query</b>"
              , icon_name   => 'media-playback-stop'
              , coderef     => sub { $self->activity_cancel_query }
            }
          , kill_query      => {
                type        => 'button'
              , markup      => "<b><span color='red'>kill session</span></b>"
              , icon_name   => 'edit-delete'
              , coderef     => sub { $self->activity_kill_session }
           }
        };
        
        $self->{pages}->[ $page_index ]->{datasheet}->{recordset_tool_items} = [ "explain_query", "cancel_query", "kill_query" ];
        
        $treeview->grab_focus;
        
    }
    
}

sub activity_explain_query {
    
    my $self = shift;
    
    my $page_index = $self->{notebook}->get_current_page;
    
    my @query_in_list = $self->{pages}->[ $page_index ]->{results_viewer_datasheet}->get_column_value( "query" );
    my $query = $query_in_list[0];

    # TODO - add_blank_page sets the current connection and database, *but* only if the database
    # TODO   is already selected. People can get to the activity list without navigating to the DB.
    # TODO   We could parse it out and select it for them ...

    $self->add_blank_page;
    
    $page_index = $self->{notebook}->get_current_page;
    
    my $treeview = $self->{pages}->[ $page_index ]->{sql_editor};
    my $buffer = $treeview->get_buffer;
    
    $buffer->insert_at_cursor( "explain\n$query" );
    $treeview->grab_focus;
    
}

sub activity_cancel_query {
    
    my $self = shift;
    
    my $page_index      = $self->{notebook}->get_current_page;
    
    my @pid_in_list = $self->{pages}->[ $page_index ]->{results_viewer_datasheet}->get_column_value( "id" );
    my $pid = $pid_in_list[0];
    
    my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE ) || return;
    my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );
    
    my $cancel_sql      = $connection->generate_query_cancel_sql( $pid );
    
    $connection->do( $cancel_sql );
    
    $self->dialog(
        {
            title       => "Cancel Signal Sent"
          , type        => "info"
          , text        => "Successfully sent the cancel signal to PID [$pid]"
        }
    );
    
}

sub activity_kill_session {

    my $self = shift;

    my $page_index      = $self->{notebook}->get_current_page;

    my @pid_in_list = $self->{pages}->[ $page_index ]->{results_viewer_datasheet}->get_column_value( "id" );
    my $pid = $pid_in_list[0];

    my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE ) || return;
    my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );

    my $cancel_sql      = $connection->generate_session_kill_sql( $pid );

    $connection->do( $cancel_sql );

    $self->dialog(
        {
            title       => "'Session Kill' signal sent"
          , type        => "info"
          , text        => "Successfully sent the 'kill session' signal to PID [$pid]"
        }
    );

}

sub insert_statement {
    
    my $self = shift;
    
    my ( $selected_paths, $model ) = $self->{treeview}->get_selection->get_selected_rows;
    
    foreach my $selected_path ( @{$selected_paths} ) {
        
        my $iter = $model->get_iter( $selected_path );
        
        my $object_name     = $model->get( $iter, OBJECT_COLUMN );
        my $object_type     = $model->get( $iter, TYPE_COLUMN );
        my $database_name   = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name     = $self->get_selected_by_object_type( SCHEMA_TYPE );
        my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE );
        
        my $columns_results = $self->{mem_dbh}->select( "select column_name from browser_columns" );
        
        my @columns;
        
        foreach my $column_result ( @{$columns_results} ) {
            push @columns, $column_result->{column_name};
        }
        
        my $connection      = $self->get_db_connection( $connection_name, $self->get_selected_by_object_type( DATABASE_TYPE ) );
        
        my $qualified_table = $connection->db_schema_table_string( $database_name, $schema_name, $object_name );
        
        my $sql = "insert into $qualified_table\n(\n    "
            . join( "\n  , ", @columns )
            . "\n) select\n    "
            . join( "\n  , ", @columns )
            . "\nfrom <INSERT TABLE NAME HERE>";
        
        my $page_index = $self->{notebook}->get_current_page;
        
        my $treeview = $self->{pages}->[ $page_index ]->{sql_editor};
        my $buffer = $treeview->get_buffer;
        
        $buffer->insert_at_cursor( $sql );
        $treeview->grab_focus;
        
    }
    
}
    
sub get_clicked_tree_object {
    
    my ( $self, $widget ) = @_;
    
    my ( $selected_paths, $model ) = $widget->get_selection->get_selected_rows;
    
    my ( $selected_path, $iter, $type, $object_name );
    
    if ( $selected_paths && @{$selected_paths} ) {
        
        $selected_path  = $$selected_paths[0];
        $iter           = $model->get_iter( $selected_path );
        $type           = $model->get( $iter, TYPE_COLUMN );
        $object_name    = $model->get( $iter, OBJECT_COLUMN );
        
    }
    
    return ( $model, $selected_path, $iter, $type, $object_name );
    
}

sub get_selected_by_object_type {
    
    my ( $self, $object_type ) = @_;
    
    my ( $model, $iters ) = $self->get_selected_iters;
    
    foreach my $iter ( @{$iters} ) {
        
        while ( $iter ) {
            
            if ( $model->get( $iter, TYPE_COLUMN ) eq $object_type ) {
                return $model->get( $iter, OBJECT_COLUMN );
            }
            
            $iter = $self->{model}->iter_parent( $iter );
            
        }
        
    }
    
    return undef;
    
}

sub get_selected_iter_by_object_type {
    
    my ( $self, $object_type ) = @_;
    
    my ( $model, $iters ) = $self->get_selected_iters;
    
    foreach my $iter ( @{$iters} ) {
        
        while ( $iter ) {
            
            if ( $model->get( $iter, TYPE_COLUMN ) eq $object_type ) {
                return $iter;
            }
            
            $iter = $self->{model}->iter_parent( $iter );
            
        }
        
    }
    
    return undef;
    
}

sub get_selected_iters {
    
    my $self = shift;
    
    my ( $selected_paths, $model ) = $self->{treeview}->get_selection->get_selected_rows;
    
    my @iters;
    
    foreach my $selected_path ( @{$selected_paths} ) {
        push @iters, $model->get_iter( $selected_path );
    }
    
    return $model, \@iters;
    
}

sub on_undo_clicked {
    
    my $self = shift;
    
    my $page_index      = $self->{notebook}->get_current_page;
    
    $self->{pages}->[ $page_index ]->{sql_editor}->get_buffer->undo;
    
}

sub on_redo_clicked {
    
    my $self = shift;
    
    my $page_index      = $self->{notebook}->get_current_page;
    
    $self->{pages}->[ $page_index ]->{sql_editor}->get_buffer->redo;
    
}

sub on_execute_clicked {
    
    my ( $self , $page_index , $direct ) = @_;
    
    if ( ! defined $page_index ) {
        $page_index = $self->{notebook}->get_current_page;
    }

    my $target_chooser  = $self->{pages}->[ $page_index ]->{target_chooser};
    my $connection      = $target_chooser->get_db_connection;
    my $buffer          = $self->{pages}->[ $page_index ]->{sql_editor}->get_buffer;
    
    my ( $sql, $iter_1, $iter_2 );
    
    if ( ( $iter_1, $iter_2 ) = $buffer->get_selection_bounds ) {
        $sql = $buffer->get_text( $iter_1, $iter_2, 1 );
    } else {
        $sql = $self->get_buffer_value( $self->{pages}->[ $page_index ]->{sql_editor} );
    }
    
    # We need to decide, now ( ie before executing anything ), whether to pass this SQL
    # to a datasheet, or whether to just execute it ourself.
    
    $sql =~ s/^[\s]*//mg;   # strip leading spaces
    $sql =~ s/^\n$//mg;     # strip blank lines
    
    my $test_sql = $sql;
    
    $test_sql =~ s/^#.*\n//; # strip 1st line ( from $test_sql only ) if it starts with a hash ( these are BigQuery modifiers )
    
    if (
        (
                1
#                $test_sql =~ /^(select|with|show|explain|describe|call|exec|pragma|list|info)/i # TODO: proper detection of SQL that returns a resultset
           || ! $connection->is_sql_database
        )
      && ! $direct
    ) {
        
        if ( exists $self->{pages}->[ $page_index ]->{results_viewer_datasheet} ) {
            eval {
                $self->{pages}->[ $page_index ]->{results_viewer_datasheet}->destroy;
            };
        }
        
        if ( ! $connection->is_sql_database ) {
            ( $connection, $sql ) = $connection->sql_to_sqlite( $sql, $self->{progress} );
        }
        
        my $sql_hash = $self->{pages}->[ $page_index ]->{datasheet}->{sql}
                     ? $self->{pages}->[ $page_index ]->{datasheet}->{sql}
                     : {
                            pass_through      => $sql
                       };
        
        my $recordset_tools_items;
        
        if ( $self->{pages}->[ $page_index ]->{datasheet}->{recordset_tool_items} ) {
            $recordset_tools_items = $self->{pages}->[ $page_index ]->{datasheet}->{recordset_tool_items}
        } elsif ( $sql_hash->{from} ) {
            $recordset_tools_items = [ qw | insert undo delete apply data_to_csv | ];
        } else {
            $recordset_tools_items = [ qw | data_to_csv | ];
        }

        my @local_fields;

        if ( exists $self->{pages}->[ $page_index ]->{datasheet} && exists $self->{pages}->[ $page_index ]->{datasheet}->{fields} ) {
            @local_fields = @{$self->{pages}->[ $page_index ]->{datasheet}->{fields}};
            $self->{pages}->[ $page_index ]->{results_viewer_datasheet} = Gtk3::Ex::DBI::Datasheet->new(
                {
                      dbh                     => $connection
                    , fields                  => \@local_fields
                    , column_sorting          => 1
                    , multi_select            => 1
                    , recordset_tool_items    => $recordset_tools_items
                    , recordset_extra_tools   => $self->{pages}->[ $page_index ]->{datasheet}->{recordset_extra_tools}
                    , sql                     => $sql_hash
                    , vbox                    => $self->{pages}->[ $page_index ]->{results_viewer_box}
                    , auto_tools_box          => 1
                    , force_editable          => 1
                }
            );
        } else {
            $self->{pages}->[ $page_index ]->{results_viewer_datasheet} = Gtk3::Ex::DBI::Datasheet->new(
                {
                      dbh                     => $connection
                    , column_sorting          => 1
                    , multi_select            => 1
                    , recordset_tool_items    => $recordset_tools_items
                    , recordset_extra_tools   => $self->{pages}->[ $page_index ]->{datasheet}->{recordset_extra_tools}
                    , sql                     => $sql_hash
                    , vbox                    => $self->{pages}->[ $page_index ]->{results_viewer_box}
                    , auto_tools_box          => 1
                    , force_editable          => 1
                }
            );
        }
        
    } else {
        
        # We want to execute this ourself, so we can avoid the Perl stack trace ...
        
        my $dbh = $connection->{connection};
        
        my $result;
        
        my $start_ts = Time::HiRes::gettimeofday;
        
        eval {
            $result = $dbh->do( $sql )
                || die( $dbh->errstr ); 
        };
        
        my $end_ts   = Time::HiRes::gettimeofday;
        
        my $err = $@;
        
        if ( $err ) {
            
            $self->dialog(
                {
                    title       => "Database server returned an error"
                  , icon        => "error"
                  , text        => $err
                }
            );
            
        } else {
        
            if ( $result eq '-1' ) {
                $self->dialog(
                    {
                        title       => "Success"
                      , type        => "info"
                      , text        => "The SQL executed successfully\nin [" . ( $end_ts - $start_ts ). "] seconds"
                    }
                );
            } else {
                $self->dialog(
                    {
                        title       => "Success"
                      , type        => "info"
                      , text        => "The SQL executed successfully \nin [" . ( $end_ts - $start_ts ). "] seconds\n"
                                     . "and returned [$result]"
                    }
                );
            }
            
        }
        
    }
    
}

sub edit_object {
    
    my $self = shift;
    
    my ( $selected_paths, $model ) = $self->{treeview}->get_selection->get_selected_rows;
    
    foreach my $selected_path ( @{$selected_paths} ) {
        
        my $iter = $model->get_iter( $selected_path );
        
        my $object_name     = $model->get( $iter, OBJECT_COLUMN );
        my $object_type     = $model->get( $iter, TYPE_COLUMN );
        my $database_name   = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name     = $self->get_selected_by_object_type( SCHEMA_TYPE );
        my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE );
        
        my $connection = $self->get_db_connection( $connection_name, $database_name ) || return;
        
        my $definition;

        if ( $object_type eq VIEW_TYPE ) {
            $definition = $connection->fetch_view( $database_name, $schema_name, $object_name ) || return;
        }

        # TODO: Materialized view

        if ( $object_type eq FUNCTION_TYPE  ) {
            $definition = $connection->fetch_function( $database_name, $schema_name, $object_name ) || return;
        }

        if ( $object_type eq PROCEDURE_TYPE  ) {
            $definition = $connection->fetch_procedure( $database_name, $schema_name, $object_name );# || return;
        }

        # Check to see if there is any text in the active page. If there is, we should create a new page ...
        if ( $self->get_buffer_value( $self->{pages}->[ $self->{notebook}->get_current_page ]->{sql_editor} ) ) {
            $self->add_page;
        }
        
        if ( $definition ) {
#            $self->{formatter}->query( $definition );
#            $definition = $self->{formatter}->beautify;
            $self->set_buffer_value( $self->{pages}->[ $self->{notebook}->get_current_page ]->{sql_editor}, $definition );
            if ( $object_type eq 'VIEW' ) {
                $self->{pages}->[ $self->{notebook}->get_current_page ]->{page_label}->set_markup(
                    "<span color='blue'><b>$object_name</b></span>" );
            }
        }
        
    }
    
}

sub refresh_objects {
    
    my $self = shift;
    
    # For now, we refresh EVERYTHING ...
    $self->build_connections;
    
}

sub refresh_materialized_view {

    my $self = shift;

    my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE );
    my ( $selected_paths, $model ) = $self->{treeview}->get_selection->get_selected_rows;

    if ( @{$selected_paths} ) {
        my $iter             = $model->get_iter( $$selected_paths[0] );
        my $database_name    = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name      = $self->get_selected_by_object_type( SCHEMA_TYPE );
        my $mview            = $model->get( $iter, OBJECT_COLUMN );
        my $connection       = $self->get_db_connection( $connection_name, $database_name ) || return;

        my $sql = $connection->refresh_materialized_view_string( $database_name, $schema_name, $mview );

        my $dbh = $connection->{connection};
        my $result;

        my $start_ts = Time::HiRes::gettimeofday;

        eval {
            $result = $connection->do( $sql )
                || die( $dbh->errstr );
        };

        my $end_ts   = Time::HiRes::gettimeofday;

        my $err = $@;

        if ( $err ) {

            $self->dialog(
                {
                    title       => "Database server returned an error"
                  , icon        => "error"
                  , text        => $err
                }
            );

        } else {

            if ( $result eq '-1' || $result eq Database::Connection::PERL_ZERO_RECORDS_INSERTED ) {
                $self->dialog(
                    {
                        title       => "Success"
                      , type        => "info"
                      , text        => "Materialized view successfully refreshed\nin [" . ( $end_ts - $start_ts ). "] seconds"
                    }
                );
            } else {
                $self->dialog(
                    {
                        title       => "Success"
                      , type        => "info"
                      , text        => "Materialized view successfully refreshed\nin [" . ( $end_ts - $start_ts ). "] seconds\n"
                                     . "and returned [$result]"
                    }
                );
            }

        }

    }

}

sub drop_object {
    
    my $self = shift;
    
    my $widget = $self->{builder}->get_object( 'browser_tree' );
    
    my ( $model, $selected_path, $iter, $type, $object_name ) = $self->get_clicked_tree_object( $widget );
    
    my $response = $self->dialog(
        {
            title       => "Drop Object!"
          , type        => "question"
          , text        => "Are you sure you want to drop the $type: $object_name?"
        }
    );
    
    if ( lc($response) eq 'yes' ) {
        
        my $connection_name     = $self->get_selected_by_object_type( CONNECTION_TYPE );
        my $database_name       = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name         = $self->get_selected_by_object_type( SCHEMA_TYPE );
        
        my $connection          = $self->get_db_connection( $connection_name, $database_name ) || return;
        
        if ( $type eq TABLE_TYPE && $connection->can( 'drop_table' ) ) {

            $connection->drop_table( $database_name , $schema_name , $object_name ) || return;

        } elsif ( $type eq DATABASE_TYPE ) {

            # We need the 'root connection' for this - can't drop a database with connections to it.
            # TODO: 1) close any of our connections to this database.
            # TODO: 2) optionally kill any other connection to this database.

            my $connection_name = $self->get_selected_by_object_type( CONNECTION_TYPE );

            my $db_type = $self->{connection_to_db_type_map}->{ $connection_name }->{DatabaseType};
            my $root_database = $self->{db_type_roots}->{ $db_type };

            $connection->disconnect();

            $connection = $self->get_db_connection(
                $connection_name
              , $root_database
            ) || return;

            my $drop_sql = $connection->drop_db_string( $database_name );

            $connection->do( $drop_sql ) || return;

        } else {
            
            my $drop_sql;
            
            if ( $type eq SCHEMA_TYPE ) {

                $drop_sql = $connection->drop_db_schema_string( $database_name , $schema_name );
                
            } elsif ( $type eq TABLE_TYPE || $type eq VIEW_TYPE || $type eq FUNCTION_TYPE || $type eq PROCEDURE_TYPE ) {
    
                $drop_sql = "drop $type " . $connection->db_schema_table_string( $database_name , $schema_name , $object_name );

            } else {
                
                $self->dialog(
                    {
                        title       => "Unsupported type"
                      , type        => "error"
                      , text        => "Dropping type [$type] is not supported. You wouldn't want us to execute an untested 'drop' statement, would you?"
                    }
                );
                
                return;
                
            }
            
            $connection->do( $drop_sql ) || return;
            
        }
        
        $self->remove_node( $iter );
        
    }
    
}

sub truncate_table {
    
    my $self = shift;
    
    my $widget = $self->{builder}->get_object( 'browser_tree' );
    
    my ( $model, $selected_path, $iter, $type, $object_name ) = $self->get_clicked_tree_object( $widget );
    
    my $response = $self->dialog(
        {
            title       => "Truncate table!"
          , type        => "question"
          , text        => "Are you sure you want to truncate the table: $object_name?"
        }
    );
    
    if ( lc($response) eq 'yes' ) {
        
        my $connection_name     = $self->get_selected_by_object_type( CONNECTION_TYPE );
        my $database_name       = $self->get_selected_by_object_type( DATABASE_TYPE );
        my $schema_name         = $self->get_selected_by_object_type( SCHEMA_TYPE );
        
        my $connection          = $self->get_db_connection( $connection_name, $database_name ) || return;
        
        my $truncate_sql = $connection->truncate_db_schema_table_string( $database_name, $schema_name, $object_name );
        
        if ( $connection->do( $truncate_sql ) ) {
            $self->dialog(
                {
                    title       => "Truncate complete"
                  , type        => "info"
                  , text        => "Table [$object_name] successfully truncated"
                }
            );
        }
        
    }
    
}

sub on_profile_table_clicked {
    
    my $self                = shift;
    
    my $connection_name     = $self->get_selected_by_object_type( CONNECTION_TYPE );
    my $database_name       = $self->get_selected_by_object_type( DATABASE_TYPE );
    my $schema_name         = $self->get_selected_by_object_type( SCHEMA_TYPE );
    my $table_name          = $self->get_selected_by_object_type( TABLE_TYPE );
    
    my $connection          = $self->get_db_connection( $connection_name, $database_name ) || return;
    
    my $table_fields        = $connection->fetch_field_list( $database_name, $schema_name, $table_name );
    my @stats               = ();
    
    my $number_of_fields    = scalar @{$table_fields};
    
    $self->{pulse_amount}   = 1 / $number_of_fields;
    
    my $total_records_return = $connection->select(
        "select count(*) from " . $connection->db_schema_table_string( $database_name, $schema_name, $table_name )
    );
    
    my $total_records       = $total_records_return->[0]->{count};
    
    my $min_frequency_cutoff_percent = $self->dialog(
        {
            title           => "Enter cutoff percentage"
          , markup          => "Before including a histrogram of each column, we check the count of the <b>most popular</b> value"
                             . " against the total number of records in the table. If this count is <b>less than</b> a cutoff percentage,"
                             . " we <b>exclude</b> that column from the report. This is to reduce noise in cases where we have a large"
                             . " number of values that would blow out the report to thousands of pages.\n\n"
                             . "This table has [$total_records] records in total ..."
          , type            => "input"
          , default         => "1%"
        }
    );
    
    $min_frequency_cutoff_percent =~ s/%//;
    
    my $values_limit = $self->dialog(
        {
            title           => "Enter a values limit"
          , markup          => "Enter a limit for the number of <b>values</b> profiled. We profile values in <i>descending</i> order of popularity"
          , type            => "input"
          , default         => "100"
        }
    );
    
    foreach my $field ( @{$table_fields} ) {
        
        if ( $field ne '_PREVIEW_ID' ) {
            
            $self->pulse( "Collecting stats on column: [$field]" );
            
            my $sql = "select    $field as VALUE, count(*) as COUNT_OF_FIELD, cast( count(*) / " . $total_records . "::FLOAT * 100 as DECIMAL(6,3) ) as VALUE_PERCENT, '$field' as PROFILED_FIELD\n"
              . "from      " . $connection->db_schema_table_string( $database_name, $schema_name, $table_name ) . "\n"
              . "group by  $field\n"
              . "having    count(*) > 10\n"
              . "order by  count(*) desc, $field\n"
              . "limit     $values_limit";
            
            print "\n\n$sql\n\n";
            
            my $these_stats_sth = $connection->prepare( $sql );
            
            $connection->execute( $these_stats_sth );
            
            my $these_stats = $these_stats_sth->fetchall_arrayref;
            
            print Dumper( $these_stats->[0] ) . "\n\n";
            
            # [0] - 1st record ( ie most popular value )
            # [2] - 3th field - ie the percentage count vs total count
            
            if ( $these_stats->[0]->[2] > $min_frequency_cutoff_percent ) {
                push @stats, @{$these_stats};
            } else {
                warn "Column [$field] most popular value [" . $these_stats->[0]->[0] . "] occurs [" . $these_stats->[0]->[1] . "]"
                   . " times, which is [" . $these_stats->[0]->[2] . "%] of the total records [$total_records]"; 
            }
            
        }
        
    }
    
    $self->reset_progress;
    
    print Dumper( \@stats );
    
    #use PDF::ReportWriter;
    
    use constant mm     => 72/25.4;     # 25.4 mm in an inch, 72 points in an inch
    
    my $file = "/tmp/" . $table_name . "_column_profiler.pdf";
    unlink $file;
    
    my $fields = [
                    {
                        name                => "Value"
                      , percent             => 70
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
                  , {
                        name                => "Percent"
                      , percent             => 15
                      , align               => "right"
                    }
    ];
    
    my $groups = [
                    {
                        name                => "Column"
                      , reprinting_header   => TRUE
                      , data_column         => 3
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
                            percent     => 30
                          , image       => {
                                                path    => "$self->{globals}->{paths}->{app}/reports/smart_associates.png",
                                           }
                          , align       => "left"
                          # , background  => {
                          #                       shape   => "box"
                          #                     # , colour  => "yellow"
                          #                  }
                        },
                        {
                            text                => "Data Profiler for file [$table_name], Column filter: most popular value > " . $min_frequency_cutoff_percent . "%"
                          , colour              => "blue"
                          , percent             => 70
                          , bold                => TRUE
                          , font_size           => 10
                          , align               => "right"
                          # , background          => {
                          #                             shape   => "box"
                          #                           , colour  => "darkgrey"
                          #                          }
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

sub on_size_allocate {
    
    my ( $self, $widget, $rectangle ) = @_;
    
    #$self->kick_gtk;
    
    if ( ! $self->{treeview_width} || $self->{treeview_width} != $rectangle->{width} ) { # TODO Remove on_size_allocate blocking workaround when blocking actually works
        
        $self->{treeview_width} = $rectangle->{width};
        
        foreach my $column_name ( "OBJECT_COLUMN" ) {
            
            my $column     = $self->{$column_name};
            my $this_width = $rectangle->{width} / 100 * $column->{_percent};
            
            print "$column_name width: $this_width\n";
            
            # TODO Figure out why we're getting very small values when constructing our own treeview
            # and avoid this some other way ... this works, but ... hmmmmm
            
            if ( $this_width < 1 ) {
                $this_width = 1;
            }
            
            Glib::Idle->add( sub {
                $column->set_fixed_width( $this_width );
                $column->{_renderer}->set( 'wrap-width', $this_width );
                return FALSE;
            } );
            
        }
        
    }
    
}

sub on_columns_clicked {
    
    my ( $self ) = @_;
    
#    elsif ( $type eq COLUMN_TYPE ) {
#        my $table = $model->get( $model->iter_parent( $iter ), OBJECT_COLUMN );
#        if ( $text ) {
#            $buffer->insert_at_cursor( " , $table" . '.' . $object_name );
#        } else {
#            $buffer->set_text(
#                "select $table" . '.' . "$object_name from "
#              . $connection->db_schema_table_string( $database, undef, $self->get_selected_by_object_type( TABLE_TYPE ) ) . "\n"
#              . $connection->limit_clause( 1000 )
#             );
#        }
#    }
    
}

sub on_tree_key_press_event {

    my ( $self, $window, $event ) = @_;

    my $keyval = $event->keyval;

    if ( $keyval == 65535 ) { # TODO: find constant
        $self->drop_object();
    }

}

sub on_sourceview_key_press_event {
    
    my ( $self, $window, $event ) = @_;

    my $keyval = $event->keyval;

    if ( $event->state =~ /control-mask/ ) { # CTRL key held down

        if ( $keyval == &Gtk3::Gdk::KEY_Return ) { # Enter key pressed

            $self->on_execute_clicked;
            return TRUE; # this swallows the key-press event and stops it from propogating - otherwise the selected text will get replaced ( with a CTRL ENTER )

        } elsif ( $keyval == &Gtk3::Gdk::KEY_S || $keyval == &Gtk3::Gdk::KEY_s ) {

            $self->save_current_page;
            return TRUE; # this swallows the key-press event and stops it from propogating - otherwise the selected text will get replaced ( with a CTRL ENTER )

        } elsif ( $keyval == &Gtk3::Gdk::KEY_O || $keyval == &Gtk3::Gdk::KEY_o ) {

            $self->load_page_from_file;
            return TRUE; # this swallows the key-press event and stops it from propogating - otherwise the selected text will get replaced ( with a CTRL ENTER )

        } elsif ( $keyval == &Gtk3::Gdk::KEY_T || $keyval == &Gtk3::Gdk::KEY_t ) {

            $self->add_blank_page;
            return TRUE; # this swallows the key-press event and stops it from propogating - otherwise the selected text will get replaced ( with a CTRL ENTER )

        } elsif ( $keyval == &Gtk3::Gdk::KEY_W || $keyval == &Gtk3::Gdk::KEY_w ) {

            $self->on_page_close_clicked( $self->{notebook}->get_current_page );

        }

    }
    
    return FALSE; # we MUST return FALSE here to allow the event to propagate
    
}

sub StartAutoRefresh {

    my $self = shift;
    my $page_index      = $self->{notebook}->get_current_page;

    my $timeout_seconds =
    ( $self->dialog(
        {
              title       => "Timing ..."
            , text        => "Refresh once every <how many> seconds?"
            , type        => "input"
            , default     => 5
        }
    ) ) || 5;

    if ( $timeout_seconds !~ /^-?([\d]*)$/ ) {
        $timeout_seconds = 5;
    }

    print "Starting autorefresh ...\n";

    $self->{ 'page_' . $page_index . '_refreshing' } = TRUE;

    # The timer to actually refresh things
    $self->{ 'page_' . $page_index . '_refresh_timer' } = Glib::Timeout->add( ( $timeout_seconds * 1000 ), sub { $self->do_auto_refresh( $page_index ) } );

}

sub StopAutoRefresh {

    my $self = shift;
    my $page_index      = $self->{notebook}->get_current_page;

    $self->{ 'page_' . $page_index . '_refreshing' } = FALSE;

}

sub do_auto_refresh {

    my ( $self, $page_index ) = @_;

    if ( $self->{ 'page_' . $page_index . '_refreshing' } ) {
        $self->on_execute_clicked( $page_index );
        return TRUE;
    } else {
        return FALSE;
    }

}

sub on_configuration_clicked {
    
    my $self = shift;
    
    $self->open_window( 'window::configuration', $self->{globals} );
    
}

sub on_browser_destroy {
    
    my $self = shift;

    # We need to shuffle through all pages, and call close_page() on each one that's never been saved. This will
    # keep our page indices in order, and prevent ID clashes and other bugs which occur if we leave the page's details
    # in SQLite for pages that have never been saved.

    my $page_counter = 0;

    foreach my $page_details ( @{$self->{pages}} ) {

        if ( ! $page_details->{path} ) {
            $self->on_page_close_clicked( $page_counter );
        }

        $page_counter ++;

    }

    $self->close_window();
    
}

1;
