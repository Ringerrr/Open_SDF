package window::configuration;

use parent 'window';

use strict;
use warnings;

use JSON;

use Glib qw( TRUE FALSE );

use Data::Dumper;

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "main"
    );
    
    #$self->kick_gtk;

    # Stack switcher
    # We have to build this in code because other items in the header bar are added in code
    # and the order that we add them determines their position

    $self->{main_stack} = $self->{builder}->get_object( "main_stack" );
    $self->{main_stack_switcher} = Gtk3::StackSwitcher->new();
    $self->{main_stack_switcher}->set_stack( $self->{main_stack} );
    $self->{builder}->get_object( 'HeaderBar' )->pack_end( $self->{main_stack_switcher} );
    $self->{main_stack_switcher}->show_all;
    
    my $model = Gtk3::ListStore->new( "Glib::String", "Gtk3::Gdk::Pixbuf" );
    my $widget = $self->{builder}->get_object( 'DatabaseType' );

    my $database_types = $self->{globals}->{config_manager}->all_database_drivers;

    foreach my $db( @{$database_types} ) {
        
        my $icon_path = $self->get_db_icon_path( $db . '.png' );
        
        if ( $icon_path ) {
            $self->{icons}->{ $db } = $self->to_pixbuf( $icon_path );
        }
        
        $model->set(
            $model->append
          , 0 , $db
          , 1, $self->{icons}->{ $db }
        );
        
    }
    
    $self->create_combo_renderers( $widget, 0, 1 );
    
    $widget->set_model( $model );

    $self->setup_odbc_driver_combo;
    
    $self->{connections_list} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh             => $self->{globals}->{local_db}
          , sql             => {
                                    select            => "DatabaseType,ConnectionName"
                                  , from              => "connections"
                                  , order_by          => "ConnectionName"
                               }
          , read_only       => TRUE
          , fields          => [
                                    {
                                        name          => "DatabaseType"
                                      , header_markup =>""
                                      , x_absolute    => 50
                                      , renderer      => "image"
                                      , custom_render_functions => [
                                                                       sub { $self->render_db_icon( @_ ) }
                                                                   ]
                                    }
                                  , {
                                        name          => "ConnectionName"
                                      , x_percent     => 100
                                    }
                               ]
          , vbox            => $self->{builder}->get_object( 'current_connections_box' )
          , on_row_select   => sub { $self->on_connection_select( @_ ) }
        }
    );
    
    $self->{connections}        = Gtk3::Ex::DBI::Form->new(
        {
            dbh                 => $self->{globals}->{local_db}
          , debug               => 1
          , sql                 => {
                                        select          => "*"
                                      , from            => "connections"
                                   }
          , auto_incrementing   => 1
          , builder             => $self->{builder}
          , recordset_tools_box => $self->{builder}->get_object( 'connection_tools_box' )
          , on_changed          => sub { $self->on_connections_changed( @_ ) }
          , before_apply        => sub { print "applying ...\n" }
          , on_current          => sub { $self->{builder}->get_object( 'Password' )->set_visibility( FALSE ); }
          , on_apply            => sub { $self->{connections_list}->query }
          , auto_tools_box      => 1
        }
    );
    
    # Create a button to test the connection ...
    my $button = Gtk3::Button->new_with_label( 'test ...' );
    my $icon   = Gtk3::Image->new_from_icon_name( 'mail-send-receive', 'button' );
    $button->set_image( $icon );
    $button->set( 'always-show-image', TRUE );
    $button->signal_connect( 'button-press-event', sub { $self->test_connection( @_ ) } );
    $self->{connections}->{recordset_tools_box}->pack_end( $button, TRUE, TRUE, 2 );
    $button->show;
    
    $self->{simple_local_config}  = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh             => $self->{globals}->{local_db}
          , sql             => {
                                    select      => "ID, key, value"
                                  , from        => "simple_config"
                               }
          , fields          => [
                                    {
                                        name        => "ID"
                                      , renderer    => "hidden"
                                    }
                                  , {
                                        name        => "key"
                                      , x_percent   => 35
                                    }
                                  , {
                                        name        => "value"
                                      , x_percent   => 65
                                    }
                               ]
          , vbox            => $self->{builder}->get_object( "simple_config_local" )
          , auto_tools_box  => 1
        }
    );

    if ( $self->{globals}->{connections}->{CONTROL} ) {

        $self->{simple_config_config}  = Gtk3::Ex::DBI::Datasheet->new(
            {
                dbh             => $self->{globals}->{connections}->{CONTROL}
              , sql             => {
                                        select      => "KEY, VALUE"
                                      , from        => "simple_config"
                                   }
              , primary_keys    => [ "KEY" ]
              , fields          => [
                                        {
                                            name        => "key"
                                          , x_percent   => 35
                                        }
                                      , {
                                            name        => "value"
                                          , x_percent   => 65
                                        }
                                   ]
              , vbox            => $self->{builder}->get_object( "simple_config_config" ) # yeah, not the best name, whatever
              , auto_tools_box  => 1
            }
        );

    }

    $self->{gui_overlays} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh             => $self->{globals}->{local_db}
          , sql             => {
                                    select      => "ID, OverlayName, OverlayPath, Active"
                                  , from        => "gui_overlays"
                               }
          , fields          => [
                                    {
                                        name        => "ID"
                                      , renderer    => "hidden"
                                    }
                                  , {
                                        name        => "OverlayName"
                                      , x_percent   => 30
                                      , read_only   => 1
                                    }
                                  , {
                                        name        => "OverlayPath"
                                      , x_percent   => 70
                                    }
                                  , {
                                        name        => "Active"
                                      , x_absolute  => 100
                                      , renderer    => "toggle"
                                    }
                               ]
          , vbox            => $self->{builder}->get_object( "gui_overlays" )
          , auto_tools_box  => 1
          , recordset_tool_items  => [ "new", "import", "undo", "delete", "apply" ]
          , recordset_extra_tools => {
                                        new      => {
                                            type        => 'button'
                                          , markup      => "<span color='green'>create empty overlay\n( ie for overlay devs )</span>"
                                          , icon_name   => 'document-new'
                                          , coderef     => sub { $self->new_overlay( 'gui' ) }
                                        }
                                      , import   => {
                                            type        => 'button'
                                          , markup      => "<span color='blue'>import</span>"
                                          , icon_name   => 'drive-harddisk'
                                          , coderef     => sub { $self->import_overlay( 'gui' ) }
                                        }
            }
        }
    );
    
    $self->{etl_overlays} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh             => $self->{globals}->{local_db}
          , sql             => {
                                    select      => "ID, OverlayName, OverlayPath, Active"
                                  , from        => "etl_overlays"
                               }
          , fields          => [
                                    {
                                        name        => "ID"
                                      , renderer    => "hidden"
                                    }
                                  , {
                                        name        => "OverlayName"
                                      , x_percent   => 30
                                      , read_only   => 1
                                    }
                                  , {
                                        name        => "OverlayPath"
                                      , x_percent   => 70
                                    }
                                  , {
                                        name        => "Active"
                                      , x_absolute  => 100
                                      , renderer    => "toggle"
                                    }
                               ]
          , vbox            => $self->{builder}->get_object( "etl_overlays" )
          , auto_tools_box  => 1
          , recordset_tool_items  => [ "new", "import", "undo", "delete", "apply" ]
          , recordset_extra_tools => {
                                        new      => {
                                            type        => 'button'
                                          , markup      => "<span color='green'>create empty noverlay\n( ie for overlay devs )</span>"
                                          , icon_name   => 'document-new'
                                          , coderef     => sub { $self->new_overlay( 'etl' ) }
                                        }
                                      , import   => {
                                            type        => 'button'
                                          , markup      => "<span color='blue'>import</span>"
                                          , icon_name   => 'drive-harddisk'
                                          , coderef     => sub { $self->import_overlay( 'etl' ) }
                                        }
            }
        }
    );
        
    $self->{builder}->connect_signals( undef, $self );
    
    $self->on_DatabaseType_changed;

    return $self;
    
}

sub render_db_icon {

    my ( $self , $treeview_column , $cellrenderer_pixbif , $liststore , $iter , $something ) = @_;

    my $db_type = $liststore->get( $iter, $self->{connections_list}->column_from_column_name( "DatabaseType" ) );

    if ( defined $db_type && $db_type ne '' ) {
        $cellrenderer_pixbif->set( pixbuf => $self->{icons}->{ $db_type } );
    } else {
        $cellrenderer_pixbif->set( pixbuf => undef );
    }

    return FALSE;

}

sub import_overlay {
    
    my ( $self, $type ) = @_;
    
    my $path = $self->file_chooser(
        {
            title       => 'Select an overlay path to add'
          , action      => 'folder'
        }
    );

    if ( ! $path ) {
        return;
    }
    
    my $config;
    
    eval {
        open( FH, "<" . $path . "/config.json" ) or die ( "Could't open $path/config.json" . $! );
        {
            local $/ = undef;
            my $config_file = <FH>;
            close FH or die $!;
            $config = from_json( $config_file );
        }
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title       => "Error reading config"
              , type        => "error"
              , text        => $err
            }
        );
        return;
    }
    
    $self->{ $type . '_overlays' }->insert;
    $self->{ $type . '_overlays' }->set_column_value( "OverlayName", $config->{overlay_name} );
    $self->{ $type . '_overlays' }->set_column_value( "OverlayPath", $path );
    $self->{ $type . '_overlays' }->apply;
    
}

sub new_overlay {
    
    my ( $self, $type ) = @_;
    
    my $name = $self->dialog(
        {
            name            => "Overlay Name"
          , type            => "input"
          , text            => "Please enter an overlay name. You can NOT rename an overlay later."
        }
    );
    
    if ( $name eq '' ) {
        return;
    }
    
    my $path = $self->file_chooser(
        {
            title       => 'Overlay Path'
          , action      => 'folder'
          , text        => "Please enter a path for the new overlay"
        }
    );
    
    if ( ! $path ) {
        return;
    }
    
    if ( $type eq 'gui' ) {
        for my $subdir ( qw | builder
                              Database
                              Database/ConfigManager
                              Database/Connection
                              icons
                              icons/db
                              icons/templates
                              packages
                              packages/template
                              packages/job
                              schemas
                              window
                              window/data_loader | ) {
            mkdir( $path . "/" . $subdir );
        }
    } elsif ( $type eq 'etl' ) {
        for my $subdir ( qw | SmartAssociates
                              SmartAssociates/Database
                              SmartAssociates/Database/Connection
                              SmartAssociates/Database/Item
                              SmartAssociates/Database/Item/Batch
                              SmartAssociates/Database/Item/Job
                              SmartAssociates/ProcessingGroup
                              SmartAssociates/TemplateConfig
                              SmartAssociates/WorkCollection  | ) {
            mkdir( $path . "/" . $subdir );
        }
    }
    
    my $config = {
        overlay_name        => $name
    };
    
    eval {
        open( FH, ">" . $path . "/config.json" ) or die $!;
        print FH to_json( $config, { pretty => 1 } );
        close FH or die $!;
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title       => "Error writing overlay config"
              , type        => "error"
              , text        => $err
            }
        );
        return;
    }
    
    $self->{ $type . '_overlays' }->insert;
    $self->{ $type . '_overlays' }->set_column_value( "OverlayName", $name );
    $self->{ $type . '_overlays' }->set_column_value( "OverlayPath", $path );
    $self->{ $type . '_overlays' }->apply;
    
    $self->dialog(
        {
            title       => "Initialised"
          , type        => "info"
          , text        => "Overlay initialised"
        }
    );
    
}

sub on_connection_select {
    
    my $self = shift;
    
    my $connection_name     = $self->{connections_list}->get_column_value( "ConnectionName" );
    
    $self->{connections}->query(
        {
            where       => "ConnectionName = ?"
          , bind_values => [ $connection_name ]
        }
    );
    
}

sub get_auth_hash {
    
    my $self = shift;
    
    my $auth_hash = {
        Username            => $self->{connections}->get_widget_value( "Username" )
      , Password            => $self->{connections}->get_widget_value( "Password" )
      , Host                => $self->{connections}->get_widget_value( "Host" )
      , Port                => $self->{connections}->get_widget_value( "Port" )
      , DatabaseType        => $self->{connections}->get_widget_value( "DatabaseType" )
      , ProxyAddress        => $self->{connections}->get_widget_value( "ProxyAddress" )
      , UseProxy            => $self->{connections}->get_widget_value( "UseProxy" )
      , Database            => $self->{connections}->get_widget_value( "Database" )
      , UseBuilder          => $self->{connections}->get_widget_value( "UseBuilder" )
      , Attribute_1         => $self->{connections}->get_widget_value( "Attribute_1" )
      , Attribute_2         => $self->{connections}->get_widget_value( "Attribute_2" )
      , Attribute_3         => $self->{connections}->get_widget_value( "Attribute_3" )
      , Attribute_4         => $self->{connections}->get_widget_value( "Attribute_4" )
      , Attribute_5         => $self->{connections}->get_widget_value( "Attribute_5" )
      , ODBC_driver         => $self->{connections}->get_widget_value( "ODBC_driver" )
    };
    
    return $auth_hash;
    
}

sub on_connections_changed {
    
    my ( $self ) = @_;
    
    if ( $self->{connections}->get_widget_value( 'UseBuilder' ) ) {
        
        my $auth_hash = $self->get_auth_hash;
        
        my $dbh = Database::Connection::generate(
            $self->{globals}
          , $auth_hash
          , 1
        );
        
        my $connection_string   = $dbh->build_connection_string( $auth_hash );
        
        $self->{connections}->set_widget_value( 'ConnectionString', $connection_string );
        
    }
    
    return TRUE;
    
}

sub on_ConnectionName_changed {
    
    my $self = shift;
    
    my $connection_name = $self->get_widget_value( "ConnectionName" );
    
    if ( $connection_name eq 'METADATA' ) {
        $self->{builder}->get_object( "CreateMetadataDBs" )->set_visible( 1 );
    } else {
        $self->{builder}->get_object( "CreateMetadataDBs" )->set_visible( 0 );
    }
    
}

sub test_connection {
    
    my $self = shift;
    
    $self->{connections}->apply();
    
    my $auth_hash = $self->{globals}->{config_manager}->get_auth_values( $self->{connections}->get_widget_value( "ConnectionName" ) );
    
    my $dbh = Database::Connection::generate(
        $self->{globals}
      , $auth_hash
    );
    
    if ( $dbh ) {
        
        $self->dialog(
            {
                title       => "Connection Successful!"
              , type        => "info"
              , text        => "Be proud ... you've connected!"
            }
        );
        
    }
    
}

sub on_DatabaseType_changed {
    
    my $self = shift;
    
    my $auth_hash = $self->get_auth_hash;
    
    if ( ! $auth_hash->{DatabaseType} ) {
        return;
    }
    
    my $dbh = Database::Connection::generate(
        $self->{globals}
      , $auth_hash
      , 1
    );
    
    my $connection_label_map = $dbh->connection_label_map;
    
    foreach my $key ( keys %{$connection_label_map} ) {
        my $value = $connection_label_map->{$key};
        if ( $value ne '' ) {
            $self->{builder}->get_object( $key . '_lbl' )->set_text( $connection_label_map->{$key} );
            $self->{builder}->get_object( $key . '_frame' )->set_visible( 1 );
        } else {
            $self->{builder}->get_object( $key . '_frame' )->set_visible( 0 );
        }
    }
    
    # For SQLite, we need a browser for the database path ( Host )

    my $db_type = $self->get_widget_value( 'DatabaseType' );

    if ( $db_type eq 'SQLite' ) {
       $self->{builder}->get_object( 'BrowseForLocation' )->set_visible( 1 );
    } else {
       $self->{builder}->get_object( 'BrowseForLocation' )->set_visible( 0 );
    }
    
}

sub on_BrowseForLocation_clicked {
    
    my $self = shift;
    
    my $path = $self->file_chooser(
        {
            title       => "Select a SQLite database file"
          , type        => "file"
        }
    );
    
    if ( $path ) {
        $self->{builder}->get_object( "Host" )->set_text( $path );
    }
    
}

sub on_Password_Visible_toggled {
    
    my $self = shift;
    
    my $password_widget = $self->{builder}->get_object( 'password' );
    $password_widget->set_visibility( ! $password_widget->get_visibility );
    
}

sub dbConnectByString {
    
    # This function connects to a specified database WITHOUT using a DSN
    #    ( ie the entries in odbc.ini or .odbc.ini )
    
    my ( $self, $string ) = @_;
    
    my $dbh;
    
    eval {
        $dbh = DBI->connect( $string )
            || die( DBI->errstr );
    };
    
    if ( $@ ) {
        
        $self->dialog(
            {
                title   => "Can't connect!",
                type    => "error",
                text    => "Could not connect to database\n" . $@
            }
        );
        
        return 0;
        
    } else {
        
        return $dbh;
        
    }
    
}

sub on_CreateMetadataDBs_clicked {
    
    my $self = shift;
    
    my $auth_hash = {
        Username            => $self->{connections}->get_widget_value( "Username" )
      , Password            => $self->{connections}->get_widget_value( "Password" )
      , Host                => $self->{connections}->get_widget_value( "Host" )
      , Port                => $self->{connections}->get_widget_value( "Port" )
      , DatabaseType        => $self->{connections}->get_widget_value( "DatabaseType" )
      , ConnectionString    => $self->{connections}->get_widget_value( 'ConnectionString' )
      , ProxyAddress        => $self->{connections}->get_widget_value( "ProxyAddress" )
      , UseProxy            => $self->{connections}->get_widget_value( "UseProxy" )
      , Database            => $self->{connections}->get_widget_value( "Database" )
      , Attribute_1         => $self->{connections}->get_widget_value( "Attribute_1" )
      , Attribute_2         => $self->{connections}->get_widget_value( "Attribute_2" )
      , Attribute_3         => $self->{connections}->get_widget_value( "Attribute_3" )
      , Attribute_4         => $self->{connections}->get_widget_value( "Attribute_4" )
      , Attribute_5         => $self->{connections}->get_widget_value( "Attribute_5" )
    };
    
    my $dbh = Database::Connection::generate(
        $self->{globals}
      , $auth_hash
    );
    
    my $sdf_db_prefix = $self->{globals}->{config_manager}->simpleGet( "SDF_DB_PREFIX" );
    
    $self->{globals}->{CONTROL_DB_NAME} = $sdf_db_prefix . "_CONTROL";
    $self->{globals}->{LOG_DB_NAME}     = $sdf_db_prefix . "_LOG";
    $self->{globals}->{config_manager}->simpleSet( "SDF_DB_PREFIX", $sdf_db_prefix );
    
    if ( $dbh ) {
        
        # We need quotes to force the names to upper-case for postgres.
        # We might need to push this into DB classes if we support hosting on
        # more DB backends.
        
        $dbh->do( 'create database "' . $self->{globals}->{CONTROL_DB_NAME} . '"' );
        $dbh->do( 'create database "' . $self->{globals}->{LOG_DB_NAME} . '"' );
        
    }
    
    $self->dialog(
        {
            title       => "Done"
          , type        => "info"
          , text        => "Done executing 'create database' commands ..."
        }
    );
    
}

sub on_MaskUnmask_clicked {
    
    my $self = shift;
    
    $self->{builder}->get_object( 'Password' )->set_visibility( ! $self->{builder}->get_object( 'Password' )->get_visibility );
    
}

sub on_ODBC_driver_config_clicked {

    my $self = shift;

    my $odbc_config_dialog = $self->open_window(
        'window::odbc_config'
      , $self->{globals}
    );

}

sub setup_odbc_driver_combo {

    my $self = shift;

    my $widget = $self->{builder}->get_object( 'ODBC_driver' );

    my $model = Gtk3::ListStore->new( "Glib::String" );

    my $odbc_drivers = $self->{globals}->{local_db}->all_odbc_drivers;

    foreach my $driver ( @{$odbc_drivers} ) {
        $model->set(
            $model->append
          , 0 , $driver
        );
    }

    if ( ! $self->{_odbc_combo_setup} ) {
        $self->create_combo_renderers( $widget, 0 );
        $self->{_odbc_combo_setup} = 1;
    }

    $widget->set_model( $model );
    $widget->set_entry_text_column( 0 );

}

sub on_main_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
