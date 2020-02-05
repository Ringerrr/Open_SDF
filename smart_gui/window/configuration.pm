package window::configuration;

use parent 'window';

use strict;
use warnings;

use JSON;

use Glib qw( TRUE FALSE );

use window::configuration::Connection;

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
      , "ConnectTimeoutAdjustment"
      , "main"
    );

    $self->maximize();

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
          , on_current          => sub {
                                            $self->{builder}->get_object( 'Password' )->set_visibility( FALSE );
                                            $self->on_DatabaseType_changed();
                                       }
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

    ###############################
    # ODBC page

    my $supported_drivers = $self->{globals}->{config_manager}->all_database_drivers;

    my $type_model        = Gtk3::ListStore->new( "Glib::String" , "Gtk3::Gdk::Pixbuf" );
    my $odbc_config_model = Gtk3::ListStore->new( "Glib::String" , "Glib::String" );

    $widget = $self->{builder}->get_object( 'global_odbc_editor.type' );

    # For the global type model combo, we have a 'Global' option, at the top of the list, for ODBC options
    # that are available to all drivers
    my $global_icon = $self->{builder}->get_object( 'main' )->render_icon( 'gtk-file' , 'menu' );

    $type_model->set(
        $type_model->append
      , 0 , 'Global'
      , 1 , $global_icon
    );

    my $odbc_drivers = $self->fetch_all_supported_odbc_drivers();

    foreach my $driver ( @{$odbc_drivers} ) {

        my $icon_path = $self->get_db_icon_path( $driver . '.png' );
        my $this_icon;

        if ( $icon_path ) {
            $this_icon = $self->to_pixbuf( $icon_path );
        }

        $type_model->set(
            $type_model->append
          , 0 , $driver
          , 1 , $this_icon
        );

        $odbc_config_model->set(
            $odbc_config_model->append
          , 0 , $driver
          , 1 , $driver
        );

    }

    $self->create_combo_renderers( $widget, 0, 1 );

    $widget->set_model( $type_model );

    $self->{configured_odbc_driver_options} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                    => $self->{globals}->{local_db}
          , column_sorting         => TRUE
          , sql                    => {
                                          select => "*"
                                        , from   => "odbc_driver_options"
                                        , where  => "0=1"
                                      }
          , fields                 => [
                                        {
                                            name        => "ID"
                                          , renderer    => "hidden"
                                        }
                                      , {
                                            name       => "Driver"
                                          , renderer   => "hidden"
                                        }
                                      , {
                                            name        => "Option"
                                          , x_percent   => 50
                                        }
                                      , {
                                            name        => "Value"
                                          , x_percent   => 50
                                        }
                                      , {
                                            name        => "Source"
                                          , renderer    => "hidden"
                                        }
            ]
          , on_row_select           => sub { $self->on_configured_option_select( @_ ) }
          , before_insert           => sub { $self->before_configured_odbc_driver_option_insert( @_ ) }
          , on_insert               => sub { $self->on_configured_odbc_driver_option_insert( @_ ) }
          , on_apply                => sub { $self->generate_odbcinstini_string() }
          , vbox                    => $self->{builder}->get_object( 'driver_options_box' )
          , auto_tools_box          => TRUE
          , recordset_tool_items    => [ "insert" , "undo" , "delete" , "apply" , "browse" ]
          , recordset_extra_tools   => {
                browse => {
                         type        => 'button'
                       , markup      => "<span color='blue'>browse ...</span>"
                       , icon_name   => 'gtk-find'
                       , coderef     => sub { $self->browse_for_driver() }
                }
            }
        }
    );

    $self->{configured_odbc_drivers} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                     => $self->{globals}->{local_db}
          , column_sorting          => 1
          , sql                     => {
                                            select       => "Driver"
                                          , from         => "odbc_drivers"
                                       }
          , fields                  => [
                                        {
                                            name        => "Driver"
                                          , x_percent   => 100
                                          , renderer    => "combo"
                                          , model       => $odbc_config_model
                                        }
                                       ]
          , vbox                    => $self->{builder}->get_object( 'ODBC_supported_drivers_box' )
          , auto_tools_box          => TRUE
          , on_row_select           => sub { $self->refresh_configured_odbc_driver_options( @_ ) }
          , on_apply                => sub { $self->on_apply_odbc_driver( @_ ) }
          , recordset_tool_items    => [ "install" , "uninstall" ]
          , recordset_extra_tools   => {
                install => {
                         type        => 'button'
                       , markup      => "<span color='blue'>install ...</span>"
                       , icon_name   => 'gtk-add'
                       , coderef     => sub { $self->install_odbc_driver() }
                }
              , uninstall => {
                         type        => 'button'
                       , markup      => "<span color='red'>uninstall</span>"
                       , icon_name   => 'gtk-delete'
                       , coderef     => sub { $self->delete_odbc_driver() }
                }
            }
        }
    );

    my $control_dbh = $self->{globals}->{config_manager}->sdf_connection( "CONTROL" , { suppress_dialog => 1 } );

    if ( $control_dbh ) {

        $self->{all_config_options} = Gtk3::Ex::DBI::Datasheet->new(
            {
                dbh                     => $control_dbh
              , column_sorting          => 1
              , read_only               => TRUE
              , on_row_select           => sub { $self->refresh_global_driver_options( @_ ) }
              , sql                     => {
                                                select      => "type , option_name"
                                              , from        => "odbc_driver_options"
                                              , order_by    => "case when type = 'global' then 1 else 2 end , type , option_name"
                                           }
              , fields                  => [
                                            {
                                                name        => "type"
                                              , x_percent   => 35
                                            }
                                          , {
                                                name        => "option_name"
                                              , x_percent   => 65
                                            }
                                           ]
              , vbox                    => $self->{builder}->get_object( 'all_config_options' )
            }
        );

        $self->{all_odbc_driver_options} = Gtk3::Ex::DBI::Form->new(
            {
                dbh                     => $control_dbh
              , sql                     => {
                                               select => "*"
                                             , from   => "odbc_driver_options"
                                             , where  => "0=1"
                                           }
              , builder                 => $self->{builder}
              , widget_prefix           => "global_odbc_editor."
              , on_apply                => sub { $self->{all_config_options}->query() }
              , on_delete               => sub { $self->{all_config_options}->query() }
              , recordset_tools_box     => $self->{builder}->get_object( 'global_odbc_editor.editor_tools_box' )
              , recordset_tool_items    => [ "label" , "insert" , "undo" , "delete" , "apply" , "package" ]
              , recordset_extra_tools   => {
                    package => {
                             type        => 'button'
                           , markup      => "<span color='blue'>package</span>"
                           , icon_name   => 'gtk-save-as'
                           , coderef     => sub { $self->package_odbc_options() }
                    }
                }
            }
        );

        $self->{simple_config_config}  = Gtk3::Ex::DBI::Datasheet->new(
            {
                dbh             => $control_dbh
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

    my $odbc_buffer = Gtk3::SourceView::Buffer->new_with_language( $self->{globals}->{gtksourceview_ini_language} );
    $odbc_buffer->set_highlight_syntax( TRUE );

    $odbc_buffer->set_style_scheme( $self->{globals}->{gtksourceview_scheme} );

    my $source_view = $self->{builder}->get_object( 'odbcinst_ini_contents' );

    if ( &Gtk3::MINOR_VERSION >= 16 ) {
        $source_view->set_background_pattern( 'GTK_SOURCE_BACKGROUND_PATTERN_TYPE_GRID' );
    }

    $source_view->set_buffer( $odbc_buffer );

    $self->manage_widget_value( "enable_odbcinst_ini_management" , FALSE );
    $self->manage_widget_value( "odbcinst_ini_contents" );

    $self->manage_widget_value( "autostart_postgres_cluster" , FALSE );

    my $defaut_pg_basedir = $ENV{"HOME"} . "/SDF_persisted/postgres";
    $self->manage_widget_value( "PG_BASEDIR" , $defaut_pg_basedir );

    return $self;
    
}

sub on_configured_option_select {

    my $self = shift;

    my $source      = $self->{configured_odbc_driver_options}->get_column_value( "Source" );
    my $option_name = $self->{configured_odbc_driver_options}->get_column_value( "OptionName" );

    my $help = $self->{globals}->{config_manager}->sdf_connection( "CONTROL" )->select(
        "select help from odbc_driver_options where type = ? and option_name = ?"
      , [ $source , $option_name ]
    );

    $self->set_widget_value( 'ODBC_option_help' , $help->[0]->{help} );

}

sub install_odbc_driver {

    my $self = shift;

    my $all_supported_odbc_drivers = $self->fetch_all_supported_odbc_drivers(); # TODO: filter out drivers already installed

    my $odbc_driver_name = $self->dialog(
        {
            title       => "Select a driver type to install"
          , type        => "options"
          , orientation => "vertical"
          , options     => $all_supported_odbc_drivers
          , text        => "The below is a list of supported drivers. Select the type you'd like to install."
        }
    );

    if ( $odbc_driver_name ) {

        my $installer = window::configuration::Connection::generate( $self->{globals} , $odbc_driver_name );
        my $args_hash = $installer->install();
        $self->{configured_odbc_drivers}->insert();
        $self->{configured_odbc_drivers}->set_column_value( "Driver" , $odbc_driver_name );
        $self->{configured_odbc_drivers}->apply();
        foreach my $key ( keys %{$args_hash} ) {
            $self->{configured_odbc_driver_options}->upsert_key(       'Option' , $key );
            $self->{configured_odbc_driver_options}->set_column_value( $self->{configured_odbc_driver_options}->column_name_to_sql_name( 'Value' )  , $args_hash->{ $key } );
        }
        $self->{configured_odbc_driver_options}->apply();
    }

    return TRUE;

}

sub delete_odbc_driver {

    my $self = shift;

    $self->{configured_odbc_drivers}->delete();
    $self->{configured_odbc_drivers}->apply();

}

sub on_ResetODBCDriverList_clicked {

    my $self = shift;

    $self->{globals}->{local_db}->do( "delete from odbc_drivers" );
    
    my $all_supported_odbc_drivers = $self->fetch_all_supported_odbc_drivers();
#    foreach my $driver ( qw | Teradata::ODBC Snowflake SQLServer Netezza Hive DB2::ODBC Redshift | ) {
    foreach my $driver ( @{$all_supported_odbc_drivers} ) {
        $self->{globals}->{local_db}->do( "insert into odbc_drivers ( Driver ) values ( ? )" , [ $driver ] );
    }

}

sub on_apply_odbc_driver {

    my ( $self , $item ) = @_;

    if ( $item->{status} eq 'inserted' ) {

        my $driver = $item->{model}->get( $item->{iter} , $self->{configured_odbc_drivers}->column_from_column_name( 'Driver' ) );

        # When a driver is inserted, we copy all the default options from the global metadata ...
        #  ... users can then override these if they want

        $self->{globals}->{local_db}->do( "delete from odbc_driver_options where Driver = ?" , [ $driver ] );

        my $options = $self->{globals}->{config_manager}->sdf_connection( 'CONTROL' )->select(
            "select * from odbc_driver_options where type = 'Global' or type = ? order by case when type = 'Global' then 1 else 2 end"
          , [ $driver ]
        );

        my $options_hash;

        # This loop merges in the options ( ie Global and driver-specific options ). Driver-specific ones override Globals
        foreach my $option ( @{$options} ) {
            $options_hash->{ $option->{option_name} } = $option;
        }

        foreach my $option_name ( keys %{$options_hash} ) {
            $self->{globals}->{local_db}->do(
                "insert into odbc_driver_options ( Driver , OptionName , OptionValue , Source ) values ( ? , ? , ? , ? )"
              , [
                    $driver
                  , $option_name
                  , $options_hash->{ $option_name }->{option_value}
                  , $options_hash->{ $option_name }->{type}
                ]
            );
        }

    } elsif ( $item->{status} eq 'deleted' ){

        $self->{globals}->{local_db}->do(
            "delete from odbc_driver_options where ID = ?"
          , [ $item->{primary_keys}->{ID} ]
        );

    }

    $self->refresh_configured_odbc_driver_options();
    $self->setup_odbc_driver_combo();
    $self->generate_odbcinstini_string();

}

sub fetch_all_supported_odbc_drivers {

    my $self = shift;

    my $supported_drivers = $self->{globals}->{config_manager}->all_database_drivers;

    my @return;

    foreach my $driver_class ( @{$supported_drivers} ) {

        eval { # In the 1st-run situation, loading commercial DB drivers can fail
            my $connection = Database::Connection::generate(
                $self->{globals}
                  , {
                        DatabaseType => $driver_class
                    }
                  , 1 # Don't connect
            );
            if ( $connection->has_odbc_driver() ) {
                push @return , $driver_class;
                # my $default_options = $self->{globals}->sdf_connection( 'CONTROL' )->select(
                #     "select * from odbc_driver_options"
                # );
                #
                # if ( $connection->can( 'odbc_config_options' ) ) {
                #
                #     my @odbc_options = $connection->odbc_config_options();
                #
                #     foreach my $option ( @odbc_options ) {
                #
                #         $self->{globals}->{local_db}->do(
                #             "insert into odbc_driver_options ( Driver , OptionName , OptionValue , Help , Browse ) values ( ? , ? , ? , ? , ? )"
                #             , [ $driver_class , $option->{OptionName} , $option->{OptionValue} , $option->{Help} , $option->{Browse} ]
                #         );
                #
                #     }
                #
                # }
            }
        };

        my $err = $@;

        if ( $err ) {
            warn "Failed to load $driver_class ... assuming we're in a 1st-run scenario. Error captured:\n$err"
        }

    }

    return \@return;

}

sub refresh_configured_odbc_driver_options {

    my $self = shift;

    my $driver = $self->{configured_odbc_drivers}->get_column_value( "Driver" );

    $self->{configured_odbc_driver_options}->query(
        {
            where       => "Driver = ?"
          , bind_values => [ $driver ]
        }
    );

}

sub refresh_global_driver_options {

    my $self = shift;

    my $type = $self->{all_config_options}->get_column_value( "type" );
    my $options_name = $self->{all_config_options}->get_column_value( "option_name" );

    $self->{all_odbc_driver_options}->query(
        {
            where       => "type = ? and option_name = ?"
          , bind_values => [ $type , $options_name ]
        }
    );

}

sub package_odbc_options {

    my $self = shift;

    my $repository = 'core'; # While some of the metadata relates to drivers in the commercial repo, there's not much value in separating things

    my $main = $self->open_window( 'window::main' );

    my $odbc_driver_options = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from odbc_driver_options"
    );

    my $json = to_json(
        {
            odbc_driver_options => {
                pre         => [ "delete from odbc_driver_options" ]
              , data        => $odbc_driver_options
            }
        }
      , { pretty => 1 }
    );

    $main->create_package(
        $json
      , 'odbc_driver_options'
      , 'odbc_driver_options'
      , $repository
    );

}

sub before_configured_odbc_driver_option_insert {

    my $self = shift;

    my $driver = $self->{configured_odbc_drivers}->get_column_value( "Driver" );

    if ( ! $driver ) {
        $self->dialog(
            {
                title   => "Select a driver first"
              , type    => "error"
              , text    => "To insert a driver option, you first need to select a driver"
            }
        );
        return FALSE;
    }

    return TRUE;

}

sub on_configured_odbc_driver_option_insert {

    my $self = shift;

    my $driver = $self->{configured_odbc_drivers}->get_column_value( "Driver" );

    $self->{configured_odbc_driver_options}->set_column_value( 'Driver' , $driver );

}

sub generate_odbcinstini_string {

    my $self = shift;

    # Here we generate the contents of the odbcinst.ini file. If management of the ~/.odbcinst.ini file is enabled,
    # when a GUI or runtime process is launched, it will check if ~/.odbcinst.ini exists, and if it doesn't, it will
    # be written, from the contents that we generate here. We do this because under Flatpak ( which is the only
    # kind of installation we can realistically support ), the home directory is *not* persisted ( apart from
    # ~/SDF_persisted ), and so we need to generate this file on launch.

    my $odbcinst_ini_string = "# This file is generated by Smart Data Frameworks ( SDF ),\n"
                            . "#  and will be re-generated when the next SDF process is launched.\n\n"
                            . "[ODBC Drivers]\n";

    my $drivers = $self->{globals}->{local_db}->select(
        "select Driver from odbc_driver_options group by Driver"
    );

    foreach my $driver ( @{$drivers} ) {
        $odbcinst_ini_string .= $driver->{Driver} . " = Installed\n";
    }

    $odbcinst_ini_string .= "\n\n";

    my $odbc_driver_options = $self->{globals}->{local_db}->select(
        "select * from odbc_driver_options order by Driver , case when Source = 'Global' then 1 else 2 end , OptionName"
    );

    my $this_driver;

    foreach my $option ( @{$odbc_driver_options} ) {
        if ( ! $this_driver || $this_driver ne $option->{Driver} ) {
            # Next driver. Write driver header.
            $this_driver = $option->{Driver};
            $odbcinst_ini_string .= "\n[$this_driver]\n";
        }
        $odbcinst_ini_string .= $option->{OptionName} . "=" . $option->{OptionValue} . "\n";
    }

    $self->set_widget_value( 'odbcinst_ini_contents' , $odbcinst_ini_string );

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
        ConnectionName      => $self->{connections}->get_widget_value( "ConnectionName" )
      , Username            => $self->{connections}->get_widget_value( "Username" )
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
      , undef
      , undef
      , undef
      , undef
      , 1 # dont_cache
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

    return TRUE;
    
}

sub on_DatabaseType_changed {
    
    my $self = shift;
    
    my $auth_hash = $self->get_auth_hash;
    
    if ( ! $auth_hash->{DatabaseType} ) {
        return;
    }
    
    $self->{selected_dbh} = Database::Connection::generate(
        $self->{globals}
      , $auth_hash
      , 1
    );
    
    my $connection_label_map = $self->{selected_dbh}->connection_label_map;
    
    foreach my $key ( keys %{$connection_label_map} ) {
        my $value = $connection_label_map->{$key};
        if ( $value ne '' ) {
            $self->{builder}->get_object( $key . '_lbl' )->set_markup( "<b>" . $connection_label_map->{$key} . "</b>" );
            $self->{builder}->get_object( $key . '_frame' )->set_visible( 1 );
        } else {
            $self->{builder}->get_object( $key . '_frame' )->set_visible( 0 );
        }
    }

    if ( $self->{selected_dbh}->connection_browse_title() ) {
       $self->{builder}->get_object( 'BrowseForLocation' )->set_visible( 1 );
    } else {
       $self->{builder}->get_object( 'BrowseForLocation' )->set_visible( 0 );
    }
    
}

sub on_BrowseForLocation_clicked {
    
    my $self = shift;

    my $browse_title = $self->{selected_dbh}->connection_browse_title();

    my $path = $self->file_chooser(
        {
            title       => $browse_title
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

    if ( ! exists $self->{_suppress_dialog} ) {
        $self->dialog(
            {
                title       => "Done"
              , type        => "info"
              , text        => "Done executing 'create database' commands ..."
            }
        );
    }

}

sub on_MaskUnmask_clicked {
    
    my $self = shift;
    
    $self->{builder}->get_object( 'Password' )->set_visibility( ! $self->{builder}->get_object( 'Password' )->get_visibility );
    
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

sub on_Initialize_Postgres_Cluster_clicked {

    my $self = shift;

    my $basedir = $self->get_widget_value( 'PG_BASEDIR' );
    my $init_cmd_output = `initdb -D $basedir`;
    $self->set_widget_value( 'Postgres_Logs' , $init_cmd_output );

    $self->set_widget_value( 'autostart_postgres_cluster' , TRUE );

    my $output = $self->{globals}->{config_manager}->start_postgres_cluster();

    $self->set_widget_value( 'Postgres_Logs' , $init_cmd_output . "\n" . $output );

    $self->{connections_list}->select_rows(
        {
            column_no   => $self->{connections_list}->column_from_sql_name( "ConnectionName" )
          , operator    => "eq"
          , value       => 'METADATA'
        }
    );

    if ( $self->{connections}->get_widget_value( "ConnectionName" ) ne 'METADATA' ) {
        $self->{connections}->insert();
    }

    my $username = getpwuid($<);

    $self->{connections}->set_widget_value( "ConnectionName", "METADATA" );
    $self->{connections}->set_widget_value( "DatabaseType", "Postgres" );
    $self->{connections}->set_widget_value( "Host" , "localhost" );
    $self->{connections}->set_widget_value( "Username" , $username );
    $self->{connections}->set_widget_value( "Password" , $username ); # Not used, but whatever
    $self->{connections}->apply();

    $self->{_suppress_dialog} = TRUE;
    $self->on_CreateMetadataDBs_clicked();
    delete $self->{_suppress_dialog};

}

sub on_Start_Postgres_Cluster_clicked {

    my $self = shift;

    my $output = $self->{globals}->{config_manager}->start_postgres_cluster();
    $self->set_widget_value( 'Postgres_Logs' , $output );

}

sub on_Stop_Postgres_Cluster_clicked {

    my $self = shift;

    my $output = $self->{globals}->{config_manager}->stop_postgres_cluster();
    $self->set_widget_value( 'Postgres_Logs' , $output );

}

sub on_main_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
