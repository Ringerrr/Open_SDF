package window::configuration::Redshift;

use 5.20.0;

use warnings;
use strict;

use parent 'window::configuration::Connection';

use Archive::Tar::Wrapper;
use Cwd;
use File::Basename;
use File::Path qw / make_path rmtree /;

use Glib qw( TRUE FALSE );

sub driver_prompt_text {

    my $self = shift;

    return "Download the 64-bit deb package from https://docs.aws.amazon.com/redshift/latest/mgmt/install-odbc-driver-linux.html.";
    
}

sub do_installation {
    
    my ( $self , $driver_package_path ) = @_;

    my $old_path = cwd;

    my ( $version , $tarball_name );

    eval {

        my $dirname  = dirname( $driver_package_path );
        chdir( $dirname );
        my $deb_to_tarball_output = `alien -t $driver_package_path`;
        say( $deb_to_tarball_output );

        if ( $deb_to_tarball_output =~ /(amazonredshift-64bit-([\d\.]*)\.tgz) generated/ ) {
            $tarball_name = $1;
            $version      = $2;
        } else {
            die( "Unsuccessful conversion of deb to tarball. Caught 'alien' output: [$deb_to_tarball_output]" );
        }

        my $tarball_path = $dirname . "/" . $tarball_name;

        my $arch = Archive::Tar::Wrapper->new();
        $arch->read( $tarball_path );
        
        # Iterate over all entries in the archive
        $arch->list_reset(); # Reset Iterator
        my $tar_tmp_dir = $arch->tardir();
        my $driver_toplevel = $tar_tmp_dir . "/opt/amazon/redshiftodbc";
        my $driver_target_path = $ENV{'HOME'} . "/SDF_persisted/.drivers";
        rmtree( $driver_target_path . "/redshiftodbc" );
        my $move_output = `mv $driver_toplevel $driver_target_path/redshiftodbc`;
        say( $move_output );

        # The amazon.redshiftodbc.ini file has a reference to /opt/amazon/redshiftodbc
        #  ... and we have to change it to ~/SDF_persisted/.drivers/redshiftodbc
        chdir( "$driver_target_path/redshiftodbc/lib/64/" );

        my $old_amazon_path = "/opt/amazon";
        $old_amazon_path =~ s/\//\\\//g;
        my $new_amazon_path = $ENV{'HOME'} . "/SDF_persisted/.drivers";
        $new_amazon_path =~ s/\//\\\//g;

        my $sed_cmd = "sed -i 's/$old_amazon_path/$new_amazon_path/g' amazon.redshiftodbc.ini";
        say( "sed cmd:\n $sed_cmd" );
        my $sed_cmd_output = `$sed_cmd`;
        say( $sed_cmd_output );

    };

    chdir( $old_path );

    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title   => "Error during installing"
              , type    => "error"
              , text    => $err
            }
        );
    }
    
    return {
        Driver  => $ENV{'HOME'} . "/SDF_persisted/.drivers/redshiftodbc/lib/64/libamazonredshiftodbc64.so"
    };
    
}

1;
