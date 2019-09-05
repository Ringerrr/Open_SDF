package window;

use strict;
use warnings;

use 5.20.0;

use Carp qw ' carp croak cluck confess longmess ';
use Glib qw ' TRUE FALSE ';
use Net::SFTP::Foreign;
use JSON qw 'encode_json decode_json ';
use XML::Parser;

my @all_GtkPaned_names;

sub dialog {
    
    my ( $self, $options ) = @_;
    
    # TODO: port to Gtk3::Dialog as Gtk3 developers are
    #       getting cranky about the use of images
    
    my $buttons = 'GTK_BUTTONS_OK';
    
    if ( $options->{type} eq 'options' || $options->{type} eq 'input' ) {
        $buttons = 'GTK_BUTTONS_OK_CANCEL';
    } elsif ( $options->{type} eq 'question' ) {
        $buttons = 'GTK_BUTTONS_YES_NO';
    }
    
    my $parent_window;
    
    if ( $options->{parent_window} ) {
        $parent_window = $options->{parent_window};
    } elsif ( $self ) {
        $parent_window = $self->get_window;
    }
    
    my $gtk_dialog_type;
    
    if (  $options->{type} eq 'options' || $options->{type} eq 'input' ) {
        $gtk_dialog_type = 'question';
    } elsif ( $options->{type} eq 'textview' ) {
        $gtk_dialog_type = 'other';
    } else {
        $gtk_dialog_type = $options->{type};
    }
    
    my $dialog = Gtk3::MessageDialog->new(
        $parent_window 
      , [ qw/modal destroy-with-parent/ ]
      , $gtk_dialog_type
      , $buttons
    );

    if ( $options->{geometry} ) {
        $dialog->set_size_request( $options->{geometry}->{x} , $options->{geometry}->{y} );
    }

    if ( $options->{title} ) {
        $dialog->set_title( $options->{title} );
    }
    
    if ( $options->{type} ne 'textview' && $options->{text} ) {
        $dialog->set_markup( window::escape( undef, $options->{text} ) );
    } elsif ( $options->{type} ne 'textview' && $options->{markup} ) {
        $dialog->set_markup( $options->{markup} );
    }
    
    my ( @radio_buttons, $entry, $sw, $textview );
    
    if ( $options->{type} eq 'options' ) {
        
        my $message_area = $dialog->get_message_area;

        my ( $box , $sw );

        if ( $options->{orientation} eq 'vertical' ) {
            $box = Gtk3::Box->new( 'GTK_ORIENTATION_VERTICAL', 0 );
            $sw = Gtk3::ScrolledWindow->new();
            $sw->set_size_request( 600, 300 ); # can't see anything unless we do this
        } else {
            $box = Gtk3::Box->new( 'GTK_ORIENTATION_HORIZONTAL', 0 );
        }
        
        foreach my $option ( @{$options->{options}} ) {
            
            my $radio_button = Gtk3::RadioButton->new_with_label_from_widget( $radio_buttons[0], $option );
            push @radio_buttons, $radio_button;
            $box->pack_end( $radio_button, TRUE, TRUE, 0);
            
        }

        if ( $sw ) {
            $sw->add( $box );
            $message_area->pack_end( $sw, TRUE, TRUE, 0 );
        } else {
            $message_area->pack_end( $box, TRUE, TRUE, 0 );
        }
        
    } elsif ( $options->{type} eq 'input' ) {
        
        my $message_area = $dialog->get_message_area;
        
        $entry = Gtk3::Entry->new;
        
        if ( exists $options->{default} ) {
            $entry->set_text( $options->{default} );
        }
        
        $message_area->pack_end( $entry, TRUE, TRUE, 0 );
        
    } elsif ( $options->{type} eq 'textview' ) {
        
        my $message_area = $dialog->get_message_area;
        
        $sw          = Gtk3::ScrolledWindow->new();
        $textview    = Gtk3::TextView->new();
        
        $sw->set_size_request( 1024, 768 ); # can't see anything unless we do this
        $sw->add( $textview );
        
        $message_area->pack_end( $sw, TRUE, TRUE, 0 );
        
        $textview->get_buffer->set_text( $options->{text} );
        
    }
    
    $dialog->show_all;

    my $response = $dialog->run;
    
    if ( $response eq 'cancel' ) {
        $dialog->destroy;
        return undef;
    }
    
    if ( $options->{type} eq 'options' ) {
        foreach my $radio_button ( @radio_buttons ) {
            if ( $radio_button->get_active ) {
                $response = $radio_button->get_label;
                last;
            }
        }
    } elsif ( $options->{type} eq 'input' ) {
        $response = $entry->get_text;
    }
    
    $dialog->destroy;
    
    return $response;
    
}

sub file_chooser {
    
    my ( $self, $options ) = @_;
    
    my $action;
    
    if ( $options->{action} eq 'save' ) {
        $action = 'GTK_FILE_CHOOSER_ACTION_SAVE';
    } elsif ( $options->{action} eq 'folder' ) {
        $action = 'GTK_FILE_CHOOSER_ACTION_CREATE_FOLDER'; 
    } else {
        $action = 'GTK_FILE_CHOOSER_ACTION_OPEN';
    }
    
    my $dialog = Gtk3::FileChooserDialog->new(
        $options->{title} ? $options->{title} : 'Choose ...'
      , $options->{parent_window}
      , $action 
      , 'Accept' , 1
      , 'Cancel' , 0
    );
    
    if ( $options->{path} ) {
        $dialog->set_current_folder( $options->{path} );
        $dialog->set_filename( $options->{path} );
    }
    
    $dialog->show_all;
    
    my $response = $dialog->run;

    if ( $response ne 'accept' && ! $response ) {
        $dialog->destroy;
        return undef;
    }
    
    my $return;
    
    if ( $options->{type} eq 'file' ) {
        $return = $dialog->get_filename;
    } else {
        $return = $dialog->get_current_folder;
    }

    $dialog->destroy;
    
    return $return;
    
}

sub create_combo_model {

    my ( $self , @values ) = @_;

    my $model = Gtk3::ListStore->new( "Glib::String" , "Glib::String" );

    foreach my $value ( @values ) {
        $model->set(
            $model->append
          , 0 , $value
          , 1 , $value
        );
    }

    return $model;

}

sub create_combo_renderers {
    
    my ( $self, $widget, $text_column, $icon_column ) = @_;
    
    if ( $icon_column ) {
        my $renderer = Gtk3::CellRendererPixbuf->new;
        $widget->pack_start( $renderer, FALSE );
        $widget->set_attributes( $renderer, pixbuf => $icon_column );
    }
    
    my $renderer = Gtk3::CellRendererText->new;
    $widget->pack_start( $renderer, FALSE );
    $widget->set_attributes( $renderer, text => $text_column );
    
}

sub get_widget_value {
    
    my ( $self, $widget ) = @_;
    
    my $type = ref $widget;
    
    if (      $type eq 'Gtk3::Calendar' ) {
        
        return $self->get_calendar_value( $widget );
        
    } elsif ( $type eq 'Gtk3::Combo' || $type eq 'Gtk3::ComboBox' ) {
        
        return $self->get_combo_value( $widget );
        
    } elsif ( $type eq 'Gtk3::TextView' ) {
        
        return $self->get_buffer_value( $widget );

    } elsif ( $type eq 'Gtk3::SourceView::View' ) {

        return $self->get_buffer_value( $widget );
        
    } elsif ( $type eq 'Gtk3::Entry' ) {
        
        return $widget->get_text;
        
    } elsif ( $type eq 'Gtk3::CheckButton' ) {
        
        return ( $widget->get_active || 0 );

    } elsif ( $type eq 'Gtk3::Switch' ) {

        return $widget->get_active || 0;

    } elsif ( $type eq 'Gtk3::RadioButton' ) {

        return $widget->get_active || 0;

    } elsif ( $type eq 'Gtk3::ToggleButton' ) {

        return $widget->get_active || 0;
        
    } else {
        
        # Also support being passed widget names
        my $widget_by_name = $self->{builder}->get_object( $widget );
        
        if ( $widget_by_name ) {
            
            return $self->get_widget_value( $widget_by_name );
            
        } else {
            
            $self->dialog(
                {
                    title       => "Application Error"
                  , type        => "error"
                  , text        => "window::get_widget_value() was called but wasn't able to figure out\n"
                                 . "what to do. I was passed [$widget] which is a [$type]"
                }
            );
            
        }
        
    }
    
}

sub set_widget_value {
    
    my ( $self, $widget, $value ) = @_;
    
    my $type = ref $widget;
    
    if (      $type eq 'Gtk3::Calendar' ) {
        
        return $self->set_calendar_value( $widget, $value );
        
    } elsif ( $type eq 'Gtk3::Combo' || $type eq 'Gtk3::ComboBox' ) {
        
        return $self->set_combo_value( $widget, $value );
        
    } elsif ( $type eq 'Gtk3::TextView' ) {
        
        return $self->set_buffer_value( $widget, $value );

    } elsif ( $type eq 'Gtk3::SourceView::View' ) {

        return $self->set_buffer_value( $widget, $value );

    } elsif ( $type eq 'Gtk3::Entry' ) {
        
        return $widget->set_text( $value || '' );
        
    } elsif ( $type eq 'Gtk3::CheckButton' ) {
        
        return $widget->set_active( $value || 0 );

    } elsif ( $type eq 'Gtk3::Switch' ) {

        return $widget->set_active( $value || 0 );

    } elsif ( $type eq 'Gtk3::RadioButton' ) {

        return $widget->set_active( $value || 0 );

    } elsif ( $type eq 'Gtk3::ToggleButton' ) {

        return $widget->set_active( $value || 0 );
        
    } else {
        
        # Also support being passed widget names
        my $widget_by_name = $self->{builder}->get_object( $widget );
        
        if ( $widget_by_name ) {
            
            return $self->set_widget_value( $widget_by_name, $value );
            
        } else {
            
            $self->dialog(
                {
                    title       => "Application Error"
                  , type        => "error"
                  , text        => "window::set_widget_value() was called but wasn't able to figure out\n"
                                 . "what to do. I was passed [$widget] which is a [$type]"
                }
            );
            
        }
        
    }
    
}

sub get_combo_value {
    
    my ( $self, $combo ) = @_;
    
    if ( ! $combo ) {
        cluck( "I wan't passed a combo object or name!" );
        return undef;
    }
    
    if ( ! ref $combo ) {
        $combo = $self->{builder}->get_object( $combo );
    }
    
    if ( ! ref $combo ) {
        cluck( "I wan't passed a combo object or valid name!" );
        return;
    }
    
    my ( $active, $iter ) = $combo->get_active_iter();
    
    if ( ! $active ) {
        return undef;
    }
    
    my $model = $combo->get_model;
    
    if ( ! $model ) {
        return undef;
    }
    
    my $value;
    
    if ( $iter ) {
        $value = $model->get( $iter, 0 );
    }
    
    return $value;
    
}

sub get_calendar_value {
    
    my ( $self, $calendar ) = @_;
    
    my ( $year, $month, $day ) = $calendar->get_date;
    
    my $date;
    
    if ( $day > 0 ) {
        
        # NOTE! NOTE! Apparently GtkCalendar has the months starting at ZERO!
        # Therefore, add one to the month...
        $month ++;
        
        # Pad the $month and $day values
        $month = sprintf( "%02d", $month );
        $day = sprintf( "%02d", $day );
        
        $date = $year . "-" . $month . "-" . $day;
        
    } else {
        $date = undef;
    }
    
    return $date;
    
}

sub set_combo_value {
    
    my ( $self, $combo, $value ) = @_;
    
    if ( ! ref $combo ) {
        $combo = $self->{builder}->get_object( $combo );
    }
    
    my $model = $combo->get_model;
    
    if ( ! $model ) {
        cluck( "set_combo_value called on a combo that doesn't have a model yet" );
        return;
    }
    
    my $iter = $model->get_iter_first();
    my $found_match = FALSE;
    
    while ( $iter ) {
        my $this_value = $model->get( $iter, 0 );
        if ( ! defined $this_value && ( ! defined $value || $value eq '' ) ) {
            $combo->set_active_iter( $iter );
        } elsif ( $value eq $this_value ) {
            $combo->set_active_iter( $iter );
            last;
        }
        if ( ! $model->iter_next( $iter ) ) {
            last;
        }
    }
    
}

sub get_buffer_value {
    
    my ( $self, $widget ) = @_;
    
    if ( ! ref $widget ) {
        $widget = $self->{builder}->get_object( $widget );
    }
    
    my $buffer = $widget->get_buffer;
    my ( $start_iter, $end_iter ) = $buffer->get_bounds;
    my $value = $buffer->get_text( $start_iter, $end_iter, 1 );
    
    return $value;
    
}

sub set_buffer_value {
    
    my ( $self, $widget, $value ) = @_;
    
    if ( ! ref $widget ) {
        $widget = $self->{builder}->get_object( $widget );
    }
    
    $widget->get_buffer->set_text( defined $value ? $value : '' );
    
}

sub pulse {
    
    my ( $self, $text, $pulse_on_every_x, $alternative_progress_bar ) = @_;
    
    $self->{pulse_counter} ++;
    
    if ( defined $pulse_on_every_x ) {
        if ( $self->{pulse_counter} % $pulse_on_every_x != 0 ) {
            return;
        }
    }
    
    #print "pulse: [" . $self->{pulse_counter} . "]\n";
    
    my $progress = $alternative_progress_bar ? $alternative_progress_bar : $self->{progress};
    
    if ( $progress ) {
        $progress->set_text( $text || '' );
    } else {
        return;
    }
    
    if ( defined $text ) {
        print $text . "\n";
    }
    
    if ( ! defined $self->{pulse_fraction} ) {
        $self->{pulse_fraction} = 0;
    }
    
    if ( $self->{pulse_amount} ) {
        if ( $self->{pulse_fraction} == 1 ) {
            $self->{pulse_fraction} = 0;
        }
        $self->{pulse_fraction} += $self->{pulse_amount};
        $progress->set_fraction( $self->{pulse_fraction} );
    } else {
        $progress->pulse;
    }
    
    $self->kick_gtk;
    
#    Glib::Timeout->add( 5000, sub { $self->reset_progress; return TRUE; } );
    
}

sub reset_progress {
    
    my $self = shift;
    
    eval {
        $self->{progress}->set_fraction( 0 );
        $self->{progress}->set_text( "" );
    };
    
}

sub get_window {
    
    my $self = shift;
    
    # This is a convenience method that any window object can call to return the actual GTK3::Window object
    # regardless of it's name
    
    my $window;
    
    if ( $self->{builder} ) {
        
        foreach my $item ( $self->{builder}->get_objects() ) {
            if ( ref $item eq "Gtk3::Window" || ref $item eq "Gtk3::Dialog") {
                $window = $item;
                last;
            }
        }
        
    }
    
    return $window;
    
}

sub maximize {
    
    my $self = shift;
    
    $self->get_window->maximize;
    
}

sub hide {
    
    my $self = shift;
    
    $self->get_window->hide;
    
}

sub show {

    my $self = shift;

    $self->get_window->show;

}

sub resize_dialog {
    
    my $self = shift;
    
    my $window = $self->get_window;
    
    my ( $width, $height ) = $window->get_size();
    
    my $builder = Gtk3::Builder->new;
    $builder->add_objects_from_file( "$self->{globals}->{paths}->{builder}/resize_dialog.glade", "resize_dialog" );
    
    my $width_adjustment = Gtk3::Adjustment->new( $width, 1, 2800, 1, 10, 0 );
    my $width_spinner    = Gtk3::SpinButton->new( $width_adjustment, 1, 0 );
    $builder->get_object( 'Width_Alignment' )->add( $width_spinner );
    
    my $height_adjustment = Gtk3::Adjustment->new( $height, 1, 1500, 1, 10, 0 );
    my $height_spinner    = Gtk3::SpinButton->new( $height_adjustment, 1, 0 );
    $builder->get_object( 'Height_Alignment' )->add( $height_spinner );
    
    $builder->get_object( "Apply" )->signal_connect( 'clicked' => sub {
        my $new_width  = $width_spinner->get_value;
        my $new_height = $height_spinner->get_value;
        $window->resize( $new_width, $new_height );
        $self->{globals}->{config_manager}->simpleSet( ref $self, $new_width . 'x' . $new_height );
    } );
    
    $builder->get_object( "Cancel" )->signal_connect( 'clicked' => sub {
        $builder->get_object( 'resize_dialog' )->destroy();
    } );
    
    $builder->get_object( "resize_dialog" )->show_all;
    $builder->get_object( "resize_dialog" )->run;
    
}

sub open_window {
    
    my ( $self, $class, $globals, $options ) = @_;
    
    # This function EITHER creates a new window of class $class, OR
    # switches to an existing instance of $class
    
    # If you don't want this functionality ( ie you want to be able to
    # create multiple instances of a window, simple create the object
    # yourself and don't use this function )
    
    my $window;
    
    if ( $self ) {
        
        # We can either be called by main.pl ( in which case $self will be undef )
        # or by an open window( in which case $self will be that window )
        
        if ( exists $self->{globals}->{windows}->{$class} ) {
            
            $self->{globals}->{windows}->{$class}->get_window->present;
            return $self->{globals}->{windows}->{$class};
            
        } else {

            # If we couldn't create required connections, $window could be UNDEF!
            #  ... in which case the code below will fail
            $window = window::load_window( $class, $self->{globals}, $options );
            
            if ( ! $window ) {
                return undef;
            }
            
            my $cached_size = $self->{globals}->{config_manager}->simpleGet( $class );
            
            if ( $cached_size && $cached_size =~ /(\d*)x(\d*)/ ) {
                my $width  = $1;
                my $height = $2;
                print "Setting remembered geometry: [" . $width . "x" . $height . "]";
                $window->get_window->resize( $width, $height );
            }
            
#            $window->get_window->signal_connect(
#                'key-press-event'
#              , sub { $window->handle_key_press_event( @_ ) }
#            );
            
            $self->{globals}->{windows}->{$class} = $window;
            
        }
        
        $globals = $self->{globals};
        
    } else {
        
        $window = window::load_window( $class, $globals, $options );
        
        # If we couldn't create required connections, $window could be UNDEF!
        #  ... in which case the code below will fail
        
        if ( ! $window ) {
            return undef;
        }
        
        my $cached_size = $globals->{config_manager}->simpleGet( $class );
            
        if ( $cached_size && $cached_size =~ /(\d*)x(\d*)/ ) {
            my $width  = $1;
            my $height = $2;
            print "Setting remembered geometry: [" . $width . "x" . $height . "]";
            eval { # this can fail if the window is being destroyed already
                $window->get_window->resize( $width, $height );
            };
        }
        
#        $window->get_window->signal_connect(
#            'key-press-event'
#          , sub { $window->handle_key_press_event( @_ ) }
#        );
        
        $globals->{windows}->{$class} = $window;
        
    }
    
    @all_GtkPaned_names = ();
    
    if ( $window->{options}->{builder_path} ) {
        
        my $xml_parser = XML::Parser->new( Style => 'Subs', Pkg => 'window' );
        my $whatever   = $xml_parser->parsefile( $window->{options}->{builder_path} );
        
    }
    
    foreach my $paned_name ( @all_GtkPaned_names ) {
        
        my $state_name = $class . ':' . $paned_name . ':position';
        my $state = $globals->{config_manager}->simpleGet( $state_name );
        my $widget = $window->{builder}->get_object( $paned_name );
        
        if ( $state ) {
            $widget->set_position( $state );
        }
        
        $widget->signal_connect(
            notify      => sub { $window->save_gtkPaned_state( @_, $state_name ) }
        );
        
    }
    
    if ( ! $window->{header_bar_setup} ) {
        $window->setup_header_bar;
    }

    $window->get_window->set_wmclass( $globals->{self}->{wm_class} , $globals->{self}->{wm_class} );

    return $window;
    
}

sub object {
    
#    my ( $expat, $object, $class, $gtk_object_class, $gtk_object_name, $gtk_object_name ) = @_;
    my ( $expat, $object, $class, $gtk_object_class, $something, $gtk_object_name ) = @_;
    
    if ( $gtk_object_class eq 'GtkPaned' ) {
        push @all_GtkPaned_names, $gtk_object_name;
    }
    
}

sub save_gtkPaned_state {
    
    my ( $self, $widget, $some_boolean_thing, $state_name ) = @_;
    
    $self->{globals}->{config_manager}->simpleSet( $state_name, $widget->get_position );
    
}

sub load_window {
    
    my ( $window_class, $globals, $options ) = @_;
    
    # This STATIC FUNCTION ( not a method ) dynamically requires a window from
    # our list of paths ( ie including gui_overlay_paths ), and instantiates it
    
    # It also locates the builder ( .glade ) file and passes it into the window
    # class' constructor
    
    # Convert path name into relative path
    my $class_relative_path = $window_class;
    $class_relative_path =~ s/:/\//g;
    
    # We also need the '.pm' at the end ...
    $class_relative_path .= '.pm';
    
    # Finally we need the type of window class, as the builder file shares a name
    # with it
    my ( $builder_name, $builder_path );
    
    if ( $window_class =~ /window::([\w]*)/ ) {
        $builder_name = $1;
    }
    
    my @all_paths = $globals->{local_db}->all_gui_paths;
    
    foreach my $include_path ( @all_paths ) {
        if ( -e $include_path . "/" . $class_relative_path ) {
            print "Loading Perl resource [" . $include_path . "/" . $class_relative_path . "] for window class [$window_class]\n";
            eval {
                require $include_path . "/" . $class_relative_path;
            };
            my $error = $@;
            if ( $error ) {
                warn( "$error" );
                dialog(
                    undef
                  , {
                        title       => "Compilation error!"
                      , type        => "error"
                      , text        => $error
                    }
                );
            }
        }
        if ( -e $include_path . "/builder/" . $builder_name . ".glade" ) {
            $builder_path = $include_path . "/builder/" . $builder_name . ".glade";
            print "Located GtkBuilder resource [$builder_path] for window class [$window_class]\n";
        }
    }
    
    $options->{builder_path} = $builder_path;
    
    my $window = $window_class->new( $globals, $options );
    
    return $window;
    
}

sub get_icon_path {
    
    my ( $self, $icon_name ) = @_;

    if ( ! exists $self->{globals}->{image_path_cache}->{ $icon_name } ) {

        my @all_paths = $self->{globals}->{local_db}->all_gui_paths;

        foreach my $include_path ( @all_paths ) {
            my $this_full_path = $include_path . "/icons/" . $icon_name;
            if ( -e $this_full_path ) {
                print "Loading icon resource [$this_full_path]\n";
                $self->{globals}->{image_path_cache}->{ $icon_name } = $this_full_path;
                return $this_full_path;
            }
        }

        warn "Couldn't find a resource for icon [$icon_name]";
        return undef;

    } else {

        return $self->{globals}->{image_path_cache}->{ $icon_name };

    }
    
}

sub get_db_icon_path {
    
    my ( $self, $icon_name ) = @_;
    
    # strip out colons and everything after them, eg Teradata::Native ==> Teradata
    # and also strip out extension ...
    $icon_name =~ s/(:.*|\..*)//g;
    $icon_name .= ".png"; # ... then put .png back

    if ( ! exists $self->{globals}->{image_path_cache}->{dbs}->{ $icon_name } ) {

        my @all_paths = $self->{globals}->{local_db}->all_gui_paths;

        foreach my $include_path ( @all_paths ) {
            my $this_full_path = $include_path . "/icons/dbs/" . $icon_name;
            if ( -e $this_full_path ) {
                print "Loading db icon resource [$this_full_path]\n";
                $self->{globals}->{image_path_cache}->{dbs}->{ $icon_name } = $this_full_path;
                return $this_full_path;
            }
        }

        warn "Couldn't find a resource for db icon [$icon_name]";
        return undef;

    } else {

        return $self->{globals}->{image_path_cache}->{dbs}->{ $icon_name };

    }
    
}

sub get_template_icon_path {

    my ( $self, $icon_name ) = @_;

    if ( ! exists $self->{globals}->{image_path_cache}->{templates}->{ $icon_name } ) {

        my @all_paths = $self->{globals}->{local_db}->all_gui_paths;

        foreach my $include_path ( @all_paths ) {
            my $this_full_path = $include_path . "/icons/templates/" . $icon_name;
            if ( -e $this_full_path ) {
                print "Loading template icon resource [$this_full_path]\n";
                $self->{globals}->{image_path_cache}->{templates}->{ $icon_name } = $this_full_path;
                return $this_full_path;
            }
        }

        warn "Couldn't find a resource for template icon [$icon_name]";
        return undef;

    } else {

        return $self->{globals}->{image_path_cache}->{templates}->{ $icon_name };

    }

}

sub setup_header_bar {
    
    my $self = shift;
    
    my $window = $self->get_window;
    my $globals = $self->{globals};
    
    # Set up the header bar
    my $header_bar = $self->{builder}->get_object( 'HeaderBar' );
    
    if ( $header_bar ) {
        
        my $logo = Gtk3::Image->new_from_file( $self->{globals}->{paths}->{app} . "/builder/sa_24x24.png" );
        $header_bar->pack_start( $logo );
        
        my $menu_bar = Gtk3::MenuBar->new;
        
        $header_bar->pack_start( $menu_bar );
        
        my @menu_controls;
        
        push @menu_controls, $globals->{paths}->{app} . "menu_control.json";
        
        foreach my $gui_overlay_paths ( $self->{globals}->{local_db}->gui_overlay_paths ) {
            
            my $this_overlay_path = $gui_overlay_paths . "/menu_control.json";
            
            if ( -e $this_overlay_path ) {
                push @menu_controls, $this_overlay_path;
            } else {
                print "Overlay path [$this_overlay_path] didn't contain a menu_control.json file";
            }
        }
        
        foreach my $menu_control_path ( @menu_controls ) {
            
            open MENU_CONTROL, "<" . $menu_control_path
                or die( "Failed to open menu_control\n" . $! );
            
            local $/; # read entire file
            
            my $menu_control_string = <MENU_CONTROL>;
            
            close MENU_CONTROL;
            
            my $menu_control = decode_json( $menu_control_string );
            
            my $menu_item = Gtk3::MenuItem->new( $menu_control->{name} );
            
            $menu_bar->append( $menu_item );
            
            my $menu = Gtk3::Menu->new;
            
            $menu_item->set_submenu( $menu );
            
            foreach my $menu_item ( @{ $menu_control->{items} } ) {
                
                my $this_menu_item  = Gtk3::MenuItem->new();
                
                my $this_menu_label = Gtk3::Label->new( $menu_item->{text} );
                eval { $this_menu_label->set_xalign( 0 ) }; # barfs on older gtk
                
                my $this_menu_image;

                if ( exists $menu_item->{image} ) {
                    $this_menu_image = Gtk3::Image->new_from_icon_name( $menu_item->{image}, 'GTK_ICON_SIZE_MENU' ); # GTK_ICON_SIZE_LARGE_TOOLBAR
                } elsif ( exists $menu_item->{project_image} ) {
                    my $pixbuf = $self->to_pixbuf( $self->get_icon_path( $menu_item->{project_image} ) , 16 , 16 );
                    $this_menu_image = Gtk3::Image->new_from_pixbuf( $pixbuf );
                }

                my $hbox            = Gtk3::Box->new( 'GTK_ORIENTATION_HORIZONTAL' , 5 );

                if ( $this_menu_image ) {
                    $hbox->pack_start( $this_menu_image, FALSE, TRUE, 2 );
                }

                $hbox->pack_start( $this_menu_label, TRUE, TRUE, 2 );
                
                $this_menu_item->add( $hbox );
                
                $this_menu_item->signal_connect_after( activate => sub { window::open_window( undef, $menu_item->{window}, $globals ) } );
                
                $menu->append( $this_menu_item );
                
            }
            
        }
        
        ##############################################################################
        # We used to need this under broadway - maximizing didn't work. It does now :)

        # my $button = Gtk3::Button->new;
        # my $icon   = Gtk3::Image->new_from_icon_name( 'zoom-fit-best', 'button' );
        # $button->set_image( $icon );
        # $button->set( 'always-show-image', TRUE );
        # $button->set_tooltip_markup( "Manipulate current window" );
        # $button->signal_connect( 'button-press-event', sub { $self->resize_dialog() } );
        #
        # $header_bar->pack_start( $button );

        ##############################################################################
        
        # $self->{progress} = Gtk3::ProgressBar->new;
        # $self->{progress}->set_show_text( TRUE );
        # $header_bar->pack_end( $self->{progress} );
        
        $header_bar->show_all;
        
    }
    
    $self->{progress} = $self->{builder}->get_object( 'progress_bar' );
    
    $self->{header_bar_setup} = 1;
    
}

sub handle_key_press_event {
    
    my ( $self, $window, $event ) = @_;
    
    # use Data::Dumper;
    
    # print Dumper( $event ) . "\n";
    print $event->keyval . "\n";
    print $event->state . "\n\n";
    
    # if ( $event->keyval == &Gtk3::Gdk::KEY_L ) {
    #     print "L pressed\n";
    # }
    
    return FALSE; # we MUST return FALSE here to allow the event to propagate
    
}

sub close_window {
    
    my ( $self, $class ) = @_;
    
    # This function destroys the window of class $class.
    # It then checks if there are any remaining windows open.
    # If not, it exits the app.

    say( "In close_window()" );

    if ( ! $class ) {
        $class = ref $self;
    }
    
    if ( ! exists $self->{globals}->{windows}->{$class} ) {
        
        print "UI::close_window() was asked to close window [$class] ... but this doesn't exist in the globals hash!\n";
        return;
        
    } else {
        
        my $window = $self->get_window();
        
        if ( $window ) {
            say( "Destroying window ..." );
            $window->destroy;
        }
        
        delete $self->{globals}->{windows}->{$class};

        my @remaining_windows = keys %{$self->{globals}->{windows}};

        say( "Remaining windows:\n    " . join( "\n  , ", @remaining_windows ) );

        if ( ! @remaining_windows && ! $self->{globals}->{in_full_restart} ) {

            say( "Last window closed ... exiting ..." );

            if ( $self->{glbobals}->{self}->{flatpak} ) {
                say( "Killing any tail processes ..." );
                system( "killall tail" );
            }

            Gtk3::main_quit();
            
        } elsif ( @remaining_windows == 1 ) {

            # If there's only 1 window remaining ... make sure it's not hidden, or there will be no
            # way to unhide it. We sometimes hide windows ( eg we open the main ETL window, then hide it, in some cases )
            
            my $window = $self->{globals}->{windows}->{ $remaining_windows[0] };
            $window->show();

        }
        
    }
    
}

sub close_all_windows {
    
    my $self = shift;
    
    foreach my $class ( keys %{$self->{globals}->{windows}} ) {
        $self->close_window( $class );
    }
    
}

sub escape {
    
    my ( $self, $text ) = @_;
    
    return Glib::Markup::escape_text( $text );
    
}

sub image_button {
    
    my ( $self, $text, $image_name ) = @_;
    
    my $button      = Gtk3::Button->new();
    my $image       = Gtk3::Image->new_from_pixbuf( $self->get_window->render_icon( $image_name, 'menu' ) );
    my $label       = Gtk3::Label->new( $text );
    my $box         = Gtk3::Box->new( 'GTK_ORIENTATION_HORIZONTAL' , 5 );
    
    $image->set_halign( 'GTK_ALIGN_END' );
    $label->set_halign( 'GTK_ALIGN_START' );
    
    $box->pack_start( $image, TRUE, TRUE, 2 );
    $box->pack_start( $label, TRUE, TRUE, 2 );
    $button->add( $box );
    
    return $button;
    
}

sub full_restart {
    
    my $self = shift;
    
    $self->{globals}->{in_full_restart} = 1;
    
    $self->close_all_windows();
    
    exec( 'perl'
        , $self->{globals}->{self}->{path}
        , $self->{globals}->{self}->{command_line_args}
    );
    
}

sub open_pdf {
    
    my ( $self, $pdf_path ) = @_;

    system( "evince $pdf_path &" );

}

sub sftp_get {
    
    my ( $self, $host, $path, $optional_username, $optional_port ) = @_;
    
    my $filename;
    
    $path =~ /.*\/(.*)/;
    $filename = $1;
    
    my $username = $optional_username
                 ? $optional_username
                 : $self->{globals}->{config_manager}->simpleGet( 'remote_etl_username' );
    
    eval {
        
        my $sftp = Net::SFTP::Foreign->new(
            $host
          , port        => ( $optional_port || 22 )
#          , user        => ( $optional_username || $ENV{USER} )
          , user        => $username
          , key_path    => $self->{globals}->{config_manager}->simpleGet( "ssh_key_path" )
        ) || die( $! );
        
        $sftp->get(
            $path
          , "/tmp/$1"
        ) || die( "Failed to transfer file:\n" . $sftp->error );
        
    };
    
    my $error = $@;
    
    if ( $error ) {
        
        $self->dialog(
            {
                title       => "sftp_get failed!"
              , type        => "error"
              , text        => $error
            }
        );
        
        return undef;
        
    } else {
        
        return "/tmp/$1";
        
    }
    
}

sub remember_widget_value {
    
    my ( $self, $widget_name ) = @_;
    
    my $value = $self->get_widget_value( $widget_name );
    
    my $class = ref $self;
    
    $self->{globals}->{config_manager}->simpleSet( $class . ':' . $widget_name, $value );
    
}

sub restore_widget_value {
    
    my ( $self, $widget_name ) = @_;
    
    my $class = ref $self;
    
    my $value = $self->{globals}->{config_manager}->simpleGet( $class . ':' . $widget_name );
    
    $self->set_widget_value( $widget_name, $value );
    
    return $value;
    
}

sub manage_widget_value {
    
    my ( $self, $widget_name, $default_value ) = @_;
    
    my $value = $self->restore_widget_value( $widget_name );
    
    if ( $value eq '' && $default_value ) {
        $value = $default_value;
        $self->set_widget_value( $widget_name, $value );
    }
    
    my $widget = $self->{builder}->get_object( $widget_name );
    
    my $signal_name;

    if ( ref $widget eq 'Gtk3::CheckButton' ) {
        $signal_name = 'toggled';
    } elsif ( ref $widget eq 'Gtk3::Switch' ) {
        $signal_name = 'state-set';
    } elsif ( ref $widget eq 'Gtk3::TextView' || ref $widget eq 'Gtk3::SourceView::View' ) {
        $widget = $widget->get_buffer;
        $signal_name = 'changed';
    } else {
        $signal_name = 'changed';
    }
    
    $widget->signal_connect( $signal_name  => sub { print "$widget_name changing ..."; $self->remember_widget_value( $widget_name ); return FALSE; } );
    
    return $value;
    
}

sub timestamp {
    
    # This function returns the current time as a standard DB kinda string
    
    my $self = shift;
    
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
    
    #               print mask ... see sprintf
    return sprintf( "%04d-%02d-%02d_%02d-%02d-%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );
    
}

sub to_pixbuf {
    
    my ( $self , $file , $x , $y ) = @_;

    if ( $file ) {
        my $pixbuf = Gtk3::Image->new_from_file( $file )->get_pixbuf;
        if ( $x && $y ) {
            $pixbuf = $pixbuf->scale_simple( $x , $y , 'GDK_INTERP_HYPER' );
        }
        return $pixbuf;
    }
    
}

sub kick_gtk {
    
    my $self = shift;
    
    no warnings 'uninitialized';

    print "In kick_gtk() ...\n";
    print "GDK_BACKEND is: " . $ENV{GDK_BACKEND} . "\n";

    if ( $ENV{GDK_BACKEND} eq 'wayland' ) {
        print "Not kicking gtk under wayland\n";
        return;
    } elsif ( $ENV{GDK_BACKEND} eq 'broadway' ) {
        print "Not kicking gtk under broadway\n";
        return;
        # for my $i ( 1 .. 10 ) {
        #     warn ""
        #     Gtk3::main_iteration();
        # }
    } else {
        print "KICKING GTK\n";
        Gtk3::main_iteration() while ( Gtk3::events_pending() );
    }
    
}

sub generate {

    my ( $globals, $class, @object_constructor_args ) = @_;

    # This STATIC FUNCTION ( not a method ) will determine which subclass of
    # Database::Connection we need, and construct an object of that type

    # Convert path name into relative path
    my $class_relative_path = $class;
    $class_relative_path =~ s/:/\//g;

    # We also need the '.pm' at the end ...
    $class_relative_path .= '.pm';

    my @all_paths = $globals->{local_db}->all_gui_paths;

    foreach my $include_path (@all_paths) {
        if (-e $include_path."/".$class_relative_path) {
            print "Loading Perl resource [" . $include_path . "/" . $class_relative_path . "] for connection class [$class]\n";
            eval {
                require $include_path . "/" . $class_relative_path;
            };
            my $error = $@;
            if ( $error ) {
                warn( "$error" );
                dialog(
                    undef
                  , {
                        title  => "Compilation error!"
                        , type => "error"
                        , text => $error
                    }
                );
            }
        }
    }

    my $object = $class->new(
        $globals
      , @object_constructor_args
    );

    return $object;

}

1;
