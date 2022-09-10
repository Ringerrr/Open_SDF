package window::configuration::BigQuery;

use 5.20.0;

use warnings;
use strict;

use parent 'window::configuration::Connection';

use Archive::Tar::Wrapper;
use Cwd;
use File::Basename;
use File::Path qw / make_path rmtree /;
use File::Find::Rule;

use Glib qw( TRUE FALSE );

sub driver_prompt_text {

    my $self = shift;

    return "Download the Linux 32-bit and 64-bit tar.gz package from https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers ( search for BigQuery ODBC )";
    
}

sub do_installation {
    
    my ( $self , $driver_package_path ) = @_;

    my $old_path = cwd;

    my ( $version , $tarball_name );

    eval {

        my $dirname  = dirname( $driver_package_path );
        chdir( $dirname );

        my $arch = Archive::Tar::Wrapper->new();
        $arch->read( $driver_package_path );
        
        # Iterate over all entries in the archive
        $arch->list_reset(); # Reset Iterator
        my $tar_tmp_dir = $arch->tardir();
        my $driver_target_path = $ENV{'HOME'} . "/SDF_persisted/.drivers";
        my $bq_path = $driver_target_path . "/bigquery";
        rmtree( $bq_path );
        mkdir( $bq_path );
        
        my ( $driver_tarball_found , $ini_file_found );
        
        for my $entry ( @{ $arch->list_all() } ) {
            my ( $tar_path , $real_path ) = @$entry;
            print "Tarpath: $tar_path Tempfile: $real_path\n";
            if ( $tar_path =~ /SimbaODBCDriverforGoogleBigQuery64/ ) {
                # We found the 64bit archive - which is itself another tar.gz - we need to unpack it
                $driver_tarball_found = 1;
                my $this_arch = Archive::Tar::Wrapper->new();
                $this_arch->read( $real_path );
                my $this_dirname = $this_arch->tardir();
                my @dirs = File::Find::Rule->directory->in( $this_dirname );
                my $second_dir = $dirs[1]; # bite me
                my $move_output = `mv $second_dir/* $bq_path`;
                say( $move_output );
            }
            if ( $tar_path =~ /simba\.googlebigqueryodbc\.ini/ ) {
                $ini_file_found = 1;
                my $move_output = `mv $real_path $bq_path`;
                say( $move_output );
                chdir( "$bq_path" );
                # The simba.googlebigqueryodbc.ini file has a reference to <INSTALLDIR>
                #  ... and we have to change it to ~/SDF_persisted/.drivers/bigquery
                my $install_path = $bq_path;
                $install_path =~ s/\//\\\//g;
                my $sed_cmd = "sed -i 's/<INSTALLDIR>/$install_path/g' simba.googlebigqueryodbc.ini";
                say( "sed cmd:\n $sed_cmd" );
                my $sed_cmd_output = `$sed_cmd`;
                say( $sed_cmd_output );
            }
        }
        
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
        Driver  => $ENV{'HOME'} . "/SDF_persisted/.drivers/simba/bigquery/lib/libgooglebigqueryodbc_sb64.so"
    };
    
}

1;
