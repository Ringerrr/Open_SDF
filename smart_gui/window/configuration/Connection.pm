package window::configuration::Connection;

use 5.20.0;

use warnings;
use strict;

use parent 'window::configuration';

use Archive::Tar::Wrapper;
use File::Copy;
use File::Temp qw/ tempdir /;
use File::Path qw ' make_path rmtree ';

use Glib qw( TRUE FALSE );

# This class contains automation to install 3rd-party driver binaries

sub generate {
    
    my ( $globals , $connection_type , $options ) = @_;
    
    my $object_class            = 'window::configuration::' . $connection_type;
    
    my $installer_object;
    
    # Convert path name into relative path
    my $class_relative_path = $object_class;
    $class_relative_path =~ s/:/\//g;
    
    # We also need the '.pm' at the end ...
    $class_relative_path .= '.pm';
    
    my @all_paths = $globals->{local_db}->all_gui_paths;
    
    my $class_found;
    
    foreach my $include_path ( @all_paths ) {
        if ( -e $include_path . "/" . $class_relative_path ) {
            $class_found = 1;
            print "Loading Perl resource [" . $include_path . "/" . $class_relative_path . "] for connection type [$object_class]\n";
            eval {
                require $include_path . "/" . $class_relative_path;
            };
            my $error = $@;
            if ( $error ) {
                warn( $error );
                return;
            }
        }
    }
    
    if ( $class_found ) {
        $installer_object = $object_class->new(
            $globals
          , $options
        );
    } else {
        $installer_object = window::data_loader::Connection->new(
            $globals
          , $options
        );
    }

    $installer_object->{_type} = $connection_type;

    return $installer_object;
    
}

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    
    $self->{builder} = $options->{builder};
    
    bless $self, $class;
    
    return $self;
    
}

sub install {
    
    my ( $self ) = @_;
    
    # The basic pattern is:
    #  - display dialog asking for a particular driver package
    #  - run any custom commands to unpack the driver and install it
    #  - set the installed path in the SQLite DB?
    
    my @created = make_path(
        $ENV{'HOME'} . "/SDF_persisted/.drivers"
      , {
            verbose     => 1
        }
    );
    
    $self->dialog(
        {
            title       => "External Download Instructions"
          , type        => "info"
          , markup      => "Due to the End User Licence Agreement's terms, you are required to perform the next step manually."
                         . "\n\n" . $self->driver_prompt_text() . "\n\n"
                         . "Save the driver into the SDF_persisted folder. The next step will ask you to then select this driver that you've downloaded,"
                         . " and will perform any custom steps required to install and configure the driver for you."
        }
    );

    my $driver_package_path = $self->file_chooser(
        {
            title       => "Please select the downloaded driver ..."
          , path        => $ENV{"HOME"} . "/SDF_persisted/"
          , type        => 'file'
        }
    );

    my $args_hash = $self->do_installation( $driver_package_path );

    return $args_hash;

}

sub binary_path {

    my $self = shift;

    return $ENV{'HOME'} . "/SDF_persisted/.drivers/" . $self->{_type};

}

sub do_installation {

    my ( $self , $driver_package_path ) = @_;

    return {};

}

1;
