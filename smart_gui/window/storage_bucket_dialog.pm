package window::storage_bucket_dialog;

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
          , connection_filter    => "where DatabaseType = '" . $options->{db_type} . "'"
        }
    );

    my $response = $self->dialog(
        {
            title          => "Choose a storage bucket and prefix ..."
          , markup         => "First select the API connection, then the bucket, and finally type a prefix.\n"
                            . " Leading/trailing slashes will be automatically detected and handled."
          , type           => "input"
          , custom_widgets => $vbox
          , ok_callback    => sub { $self->ok_callback( @_) }
        }
    );
    
    # $response->{response} now has the *path*, and we want to make sure there is *no* leading slash, but there *is* a trailing slash.
    $response->{response} =~ s/^\///;   # remove leading slash
    $response->{response} =~ s|/?$|/|; # add a trailing slash if it doesn't exist
    
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
