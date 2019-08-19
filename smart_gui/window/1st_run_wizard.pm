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
        $self->{globals}->{config_manager}->simpleSet( 'ENV:FLATPAK', 1 );
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

sub on_Open_Overlays_clicked {
    
    my $self = shift;
    
    my $config_window = $self->open_window( 'window::configuration', $self->{globals} );
    
    # $config_window->{builder}->get_object( "configuration_notebook" )->set_current_page( 3 );
    $config_window->{main_stack}->set_visible_child_name( 'overlays' );

}

sub on_FirstRunComplete_clicked {
    
    my $self = shift;
    
    $self->{globals}->{config_manager}->simpleSet(
        'FIRST_RUN_COMPLETE'
      , 1
    );
    
}

sub on_Open_ReleaseManager_clicked {
    
    my $self = shift;
    
    my $config_window = $self->open_window( 'window::release_manager', $self->{globals}, { apply_all => TRUE } );
    
}

sub on_1st_run_wizard_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
