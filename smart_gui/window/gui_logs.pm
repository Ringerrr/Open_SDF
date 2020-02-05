package window::gui_logs;

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
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "gui_logs"
    );

    $self->maximize();

    if ( $self->{globals}->{STDOUT_READER} ) {

        my $log_map = {
            LogTextView   => {
                                 filehandle => $self->{globals}->{STDOUT_READER}
                               , tag        => 'blue'
                             }
          , ErrorTextView => {
                                 filehandle => $self->{globals}->{STDERR_READER}
                               , tag        => 'red'
                             }
        };

        my $bold = Glib::Object::Introspection->convert_sv_to_enum("Pango::Weight", "bold");

        foreach my $view_type ( keys %{$log_map} ) {

            $self->{ $view_type } = $self->{builder}->get_object( $view_type )->get_buffer;
            $self->{ $view_type . "_vadjustment" } = $self->{builder}->get_object( $view_type . '_scrolled_window' )->get_vadjustment;

            foreach my $colour ( qw | red blue | ) {

                $self->{ $view_type }->create_tag(
                    $colour
                  , 'weight'    => $bold
                  , foreground  => $colour
                );

            }

           open my $log , ">/tmp/io_log.txt";
           select $log;
           $| = 1;
           select STDOUT;

            Glib::IO->add_watch( fileno( $log_map->{ $view_type }->{filehandle} ) , ['in'] , sub {

                if ( $self->{closing} ) {
                    print $log "closing in add_watch()\n";
                    return FALSE;
                }

                print $log "Another hit ...\n";

                my ( $fileno, $condition ) = @_;

                my $lines;

                sysread $log_map->{ $view_type }->{filehandle}, $lines, 65536;

                foreach my $line ( split /\n/, $lines ) {
                    $line .= "\n";
                    $self->{ $view_type }->insert_with_tags_by_name( $self->{ $view_type }->get_end_iter, $line, $log_map->{ $view_type }->{tag} );
                }

                Glib::Idle->add( sub {
                    $self->{ $view_type . "_vadjustment" }->set_value( $self->{  $view_type . "_vadjustment" }->get_upper - $self->{  $view_type . "_vadjustment" }->get_page_increment - $self->{  $view_type . "_vadjustment" }->get_step_increment );
                    return FALSE; # uninstall callback
                } );

                foreach my $open_window_name ( keys %{ $self->{globals}->{windows} } ) {
                    my $this_window = $self->{globals}->{windows}->{ $open_window_name };
                    if ( $this_window->get_window->is_active() ) {
                        $this_window->pulse_log_button();
                    }
                }

                return TRUE;  # continue without uninstalling

            } );

        }

    }

    return $self;

}

sub on_gui_logs_destroy_event {

#    my $self = shift;
#
#    warn "Oh no you don't ...";
#
#    $self->hide();
#
#    return TRUE;

}

sub on_gui_logs_destroy {

    my $self = shift;

    warn "Doh!";

    $self->close_window();

}

1;
