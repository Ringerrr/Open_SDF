package window::azure_blob_storage_dialog;

use parent 'window';

use strict;
use warnings;

use Glib qw( TRUE FALSE );

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    my $vbox = Gtk3::Box->new( 'GTK_ORIENTATION_VERTICAL' , 5 );
    
    $self->{chooser} = widget::conn_db_table_chooser->new(
        $self->{globals}
      , $vbox
      , {
            database              => 1
          , schema                => 0
          , table                 => 0
        }
      , {
            # on_connection_changed => sub { $self->on_connection_changed() }
        }
      , {
            no_labels            => 1
          , connection_filter    => "where DatabaseType = 'Azure::Blob'"
        }
    );
    
    my $response = $self->dialog(
        {
            title          => "Choose an Azure Blog storage container ..."
          , markup         => "First select the Azure connection, then the container, and finally type a prefix.\n"
                            . " For the prefix, do <b>not</b> use a <b>leading</b> slash, but <b>do</b> use a trailing slash."
          , type           => "input"
          , custom_widgets => $vbox
          , ok_callback=> sub { $self->ok_callback( @_) }
        }
    );
    
    return $response;

}

sub ok_callback {
    
    my $self = shift;
    
    return {
        connection     => $self->{chooser}->get_connection_name()
      , bucket         => $self->{chooser}->get_database_name()
    };
    
}

1;
