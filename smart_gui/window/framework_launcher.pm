package window::framework_launcher;

use parent 'window';

use strict;
use warnings;

use Glib qw( TRUE FALSE );

use Data::Dumper;

use feature 'switch';

use constant        LOG_LEVEL_FATAL             => 0;
use constant        LOG_LEVEL_ERROR             => 1;
use constant        LOG_LEVEL_WARN              => 2;
use constant        LOG_LEVEL_INFO              => 3;
use constant        LOG_LEVEL_DEBUG             => 4;

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "FrameworkLauncher"
    );

    $self->maximize();

    $self->{builder}->connect_signals( undef, $self );
    
    my $font = Pango::FontDescription::from_string( 'Fixed' );
    my $textview = $self->{builder}->get_object( 'LogViewer' );
    $textview->modify_font( $font );
    
    $self->{etl_log_buffer} = $self->{builder}->get_object( "LogViewer" )->get_buffer;
    
    my $bold = Glib::Object::Introspection->convert_sv_to_enum("Pango::Weight", "bold");
    
    $self->{etl_log_buffer}->create_tag(
        LOG_LEVEL_FATAL
      , 'weight'    => $bold
      , foreground  => 'red'
    );
    
    $self->{etl_log_buffer}->create_tag(
        LOG_LEVEL_ERROR
      , 'weight'    => $bold
      , foreground  => 'pink'
    );
    
    $self->{etl_log_buffer}->create_tag(
        LOG_LEVEL_WARN
      , 'weight'    => $bold
      , foreground  => 'orange'
    );
    
    $self->{etl_log_buffer}->create_tag(
        LOG_LEVEL_INFO
      , 'weight'    => $bold
      , foreground  => 'green'
    );
    
    $self->{etl_log_buffer}->create_tag(
        LOG_LEVEL_DEBUG
      , 'weight'    => $bold
      , foreground  => 'blue'
    );
    
    $self->manage_widget_value( 'RemoteHost' );
    $self->manage_widget_value( 'RemoteUsername' );
    
    return $self;
    
}

sub on_Execute_clicked {
    
    my $self = shift;

    my $processing_group_name       = $self->{builder}->get_object( "ProcessingGroupName" )->get_text;
    my $custom_args                 = $self->get_widget_value( "CustomArgs" );
    
    my $execute_on_remote_host      = $self->{builder}->get_object( 'ExecuteOnRemoteHost' )->get_active;
    my $remote_host                 = $self->{builder}->get_object( 'RemoteHost' )->get_text;
    my $remote_username             = $self->{builder}->get_object( 'RemoteUsername' )->get_text;

    my @args;
    
    my ( $app_path, $app_name );
    
    $app_path = $self->{globals}->{paths}->{parent} . "/smart_etl";
    $app_name = "etl.pl";
    
    my $processing_group_str;
    
    $processing_group_str ="--processing-group=$processing_group_name";

    # We can probably remove this - custom args have moved into metadata, and don't get passed in on the command-line
    if ( defined $custom_args ) {
        my ( $leading_args , $json_args );
        if ( $custom_args =~ /(.*)\s*(--args=.*)/ ) {
            ( $leading_args , $json_args ) = ( $1 , $2 );
            $leading_args =~ s/\s*$//;    # strip trailing spaces
            # Now break the leading args into an array. We want to escape spaces in job ( processing group ) names,
            # but we *don't* want to escape the spaces between args
            my @all_leading_args = split( "--", $leading_args );
            foreach my $this_arg ( @all_leading_args ) {
                next if ( ! defined $this_arg || $this_arg eq '' );
                $this_arg =~ s/\s*$//g;   # strip trailing spaces
                $this_arg =~ s/\\/\\\\/g; # escape escapes
                $this_arg =~ s/"/\\"/g;   # escape quotes
                $this_arg =~ s/,/\\,/g;   # escape commas
                $this_arg =~ s/\s/\\ /g;  # escape spaces
                $this_arg = "--" . $this_arg;
            }
            $leading_args = join( " " , @all_leading_args );
            # $leading_args =~ s/\\/\\\\/g; # escape escapes
            # $leading_args =~ s/"/\\"/g;   # escape quotes
            # $leading_args =~ s/,/\\,/g;   # escape commas
            # $leading_args =~ s/\s/\\ /g;  # escape spaces
            $json_args    =~ s/\\/\\\\/g; # escape escapes
            $json_args    =~ s/"/\\"/g;   # escape quotes
            $json_args    =~ s/,/\\,/g;   # escape commas
            $json_args    =~ s/\s/\\ /g;  # escape spaces
            $json_args    =~ s/\(/\\\(/g; # escape (
            $json_args    =~ s/\)/\\\)/g; # escape )
            $json_args    =~ s/'/\\'/g;   # escape '
            $custom_args  = $leading_args . " " . $json_args;
        } else {
            warn "Failed to pass custom args: $custom_args";
        }
        $processing_group_str .= " $custom_args";
    }

    my $log_level = $self->{builder}->get_object( 'LogLevelDebug' )->get_active ? 'debug' : 'info';
    
    $ENV{PERL5LIB} = $app_path;

    $processing_group_str .= " --log-level=" . $log_level;

    if ( ! $execute_on_remote_host ) {
        push @args
           , "-I ". $app_path
           , $app_path . "/" . $app_name
           , "--user-profile=" . $self->{globals}->{self}->{user_profile}
           , $processing_group_str;
    } else {
        
        push @args
           , 'ssh ' . $remote_username . '@' . $remote_host
           , "'flatpak run --command=/app/launch_etl.sh biz.smartassociates.sdf $processing_group_str'";
        
    }
    
    my $args_string = join( " ", @args );
    
#    $self->{globals}->{log}->print ( "\n\n$args_string\n\n" );
    print "\n\n$args_string\n\n";

    my $OUTPUT_FH;
    
    eval {
        
        if ( ! $execute_on_remote_host ) {
            
            open( $OUTPUT_FH, "cd ../smart_etl && perl $args_string |" )
                || die( "Can't fork!\n" . $! );
            
        } else {
            
            open( $OUTPUT_FH, "$args_string |" );
            
        }
        
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            title       => "Oh no!"
          , type        => "error"
          , text        => $self->escape( $err )
        );
        return;
    }

    $self->{builder}->get_object( "LogViewer" )->get_buffer->set_text( "" );
    
    $self->{vadjustment} = $self->{builder}->get_object( 'LogViewerScrolledWindow' )->get_vadjustment;
    
    $self->{output} = [];
    
    Glib::IO->add_watch( fileno( $OUTPUT_FH ), ['in', 'hup'], sub {
        
        print "inside IO watch\n";
        
        my ( $fileno, $condition ) = @_;
        
        my $lines;
        
        sysread $OUTPUT_FH, $lines, 65536;

        $self->log_to_buffer( $lines );

        # foreach my $line ( split /\n/, $lines ) {
        #
        #     $line .= "\n";
        #
        #     if ( $line =~ /[\d_]*\s(\d)\s([\d\w]*)\s([\.\w\/-]*):(\d*)\s(.*)/ ) {
        #         my $severity    = $1;
        #         my $job_id      = $2;
        #         my $app         = $3;
        #         my $line_no     = $4;
        #         my $msg         = $5;
        #         $self->{etl_log_buffer}->insert_with_tags_by_name( $self->{etl_log_buffer}->get_end_iter, $line, $severity );
        #     } else {
        #         $self->{etl_log_buffer}->insert( $self->{etl_log_buffer}->get_end_iter, $line );
        #     }
        #
        #     push @{$self->{output}}, $line;
        #
        # }
        
        Glib::Idle->add( sub {
            $self->{vadjustment}->set_value( $self->{vadjustment}->get_upper - $self->{vadjustment}->get_page_increment - $self->{vadjustment}->get_step_increment );
            return FALSE; # uninstall callback
        } );
        
        if ( $condition >= 'hup' ) {
            close $OUTPUT_FH;
            Glib::Timeout->add( 1000, sub { $self->open_etl_monitor } );
            return FALSE; # uninstall
        } else {
            return TRUE;  # continue without uninstalling
        }
        
    } );
    
}

sub log_to_buffer {

    my ( $self , $lines ) = @_;

        foreach my $line ( split /\n/, $lines ) {

        $line .= "\n";

        if ( $line =~ /[\d_]*\s(\d)\s([\d\w]*)\s([\.\w\/-]*):(\d*)\s(.*)/ ) {
            my $severity    = $1;
            my $job_id      = $2;
            my $app         = $3;
            my $line_no     = $4;
            my $msg         = $5;
            $self->{etl_log_buffer}->insert_with_tags_by_name( $self->{etl_log_buffer}->get_end_iter, $line, $severity );
        } else {
            $self->{etl_log_buffer}->insert( $self->{etl_log_buffer}->get_end_iter, $line );
        }

        push @{$self->{output}}, $line;

    }

}

sub open_etl_monitor {
    
    my $self = shift;
    
    # Now we walk backwards in the log, from the last line, looking for the line near the bottom that logs the batch number
    
    my $batches;
    
    my @output = @{$self->{output}};
    
    my $log_line = $#output;
    
    while ( $log_line > 1 ) {
        
        if ( $output[$log_line] =~ /.*Batch\s\[(\d*)\].*/ ) {
            $batches->{ $1 } ++;
            $log_line --;
        } else {
            $log_line --;
        }
        
    }
    
    if ( keys %{$batches} ) {
        my $etl_monitor = $self->open_window( 'window::etl_monitor' );
        $etl_monitor->{builder}->get_object( "BatchID" )->set_text( join( ",", keys %{$batches} ) );
        $etl_monitor->{builder}->get_object( "BatchID_Filter" )->set_active( 1 );
    }
    
    return FALSE;
    
}

sub zoom_log {

    my $self = shift;

    if ( ! $self->{log_zoom_state} ) {

        $self->{log_paned_normal_state} = $self->{builder}->get_object( 'Main_Paned' )->get_position;
        $self->{builder}->get_object( 'Main_Paned' )->set_position( 0 );

        $self->{log_zoom_state} = 1;

    } else {

        $self->{builder}->get_object( 'Main_Paned' )->set_position( $self->{log_paned_normal_state} );

        $self->{log_zoom_state} = 0;

    }

}

sub on_FrameworkLauncher_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
