package window::1st_run_wizard;

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
      , "1st_run_wizard"
    );
    
    $self->maximize;
    
    # Load any existing values into wizard
    
    $self->set_widget_value(
        'SDF_DB_PREFIX'
      , $self->{globals}->{config_manager}->simpleGet( 'SDF_DB_PREFIX' ) || 'SDF'
    );
    
    # Set flatpak-specific stuff if we've been started in flatpak mode
    if ( $self->{globals}->{self}->{flatpak} ) {
        $self->{globals}->{config_manager}->simpleSet( 'ENV:FLATPAK' , 1 );
        $self->{globals}->{config_manager}->simpleSet( 'window::configuration:enable_odbcinst_ini_management' , 1 );
        # $self->{globals}->{config_manager}->simpleSet( 'ENV:ODBCINSTINI', '/app/etc/odbcinst.ini' );
    }
    
    $self->{builder}->connect_signals( undef, $self );
    
    return $self;
    
}

sub on_SDF_DB_PREFIX_save_clicked {
    
    my $self = shift;
    
    my $sdf_db_prefix = $self->get_widget_value( 'SDF_DB_PREFIX' );
    
    $self->{globals}->{config_manager}->simpleSet(
        'SDF_DB_PREFIX'
      , $sdf_db_prefix
    );
    
    $ENV{'SDF_DB_PREFIX'} = $sdf_db_prefix;
    $self->{globals}->{CONTROL_DB_NAME} = $sdf_db_prefix . "_CONTROL";
    $self->{globals}->{LOG_DB_NAME}     = $sdf_db_prefix . "_LOG";

    # We also create the 'SDF_config' ( SQLite ) connection at this time ...
    my $config_window = $self->open_window( 'window::configuration', $self->{globals} );

    $config_window->{connections_list}->select_rows(
        {
            column_no   => $config_window->{connections_list}->column_from_sql_name( "ConnectionName" )
          , operator    => "eq"
          , value       => 'SDF_config'
        }
    );

    if ( $config_window->{connections}->get_widget_value( "ConnectionName" ) ne 'SDF_config' ) {
        $config_window->{connections}->insert();
    }

    my $sqlite_path = $self->{globals}->{paths}->{profile} . "/config.db";

    $config_window->{connections}->set_widget_value( "ConnectionName", "SDF_config" );
    $config_window->{connections}->set_widget_value( "DatabaseType", "SQLite" );
    $config_window->{connections}->set_widget_value( "Host", $sqlite_path );
    $config_window->{connections}->apply();
    $config_window->close_window();
    
}

sub on_Configure_METADATA_clicked {
    
    my $self = shift;
    
    my $config_window = $self->open_window( 'window::configuration', $self->{globals} );
    
    $config_window->{connections_list}->select_rows(
        {
            column_no   => $config_window->{connections_list}->column_from_sql_name( "ConnectionName" )
          , operator    => "eq"
          , value       => 'METADATA'
        }
    );
    
    if ( $config_window->{connections}->get_widget_value( "ConnectionName" ) ne 'METADATA' ) {
        $config_window->{connections}->insert();
        $config_window->{connections}->set_widget_value( "ConnectionName", "METADATA" );
        $config_window->{connections}->set_widget_value( "DatabaseType", "Postgres" );
    }

}

sub on_Manage_Postgres_clicked {

    my $self = shift;

    my $config_window = $self->open_window( 'window::configuration', $self->{globals} );

    $config_window->{main_stack}->set_visible_child_name( 'postgres_management' );

}

sub on_FirstRunComplete_clicked {
    
    my $self = shift;
    
    $self->{globals}->{config_manager}->simpleSet(
        'FIRST_RUN_COMPLETE'
      , 1
    );
    
    $self->dialog(
        {
            title   => "Done"
          , type    => "info"
          , text    => "Initial configuration complete. A full restart will now occur. Note that this next application startup"
                     . " will be slow, as SDF has to import all of its metadata that drives the application. Please be patient ..."
        }
    );
    
    $self->full_restart();
    
}

sub on_DefaultConfig_clicked {

    my $self = shift;

    $self->dialog(
        {
            title   => "Preparing ..."
          , type    => "info"
          , text    => "SDF will now generate a default configuration for you. This will take about 15 seconds, and you will receive another dialog box when it's complete."
        }
    );

    $self->on_SDF_DB_PREFIX_save_clicked();

    my $config_window = $self->open_window( 'window::configuration', $self->{globals} );
    $config_window->{main_stack}->set_visible_child_name( 'postgres_management' );
    $config_window->on_Initialize_Postgres_Cluster_clicked();
    $config_window->close_window();

    $self->on_FirstRunComplete_clicked();
    
}

sub on_1st_run_wizard_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
