package Database::ConfigManager::SQLite;

use strict;
use warnings;

use parent 'Database::ConfigManager';

use constant TYPE       => 'SQLite';

use Glib qw( TRUE FALSE );
use File::Find qw( finddepth );

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1], $_[2], $_[3] );
    
    my $packages = $self->{dbh}->select(
        "select OverlayName, OverlayPath from gui_overlays where Active = 1"
    );
    
    foreach my $package ( @{$packages} ) {
        $self->{gui_package_map}->{ $package->{OverlayName} } = $package->{OverlayPath};
    }
    
    $packages = $self->{dbh}->select(
        "select OverlayName, OverlayPath from etl_overlays where Active = 1"
    );
    
    foreach my $package ( @{$packages} ) {
        $self->{etl_package_map}->{ $package->{OverlayName} } = $package->{OverlayPath};
    }
    
    $self->set_environment_variables();
    
    $self->{db_connection_cache} = {};
    
    return $self;
    
}

sub create_simple_config {
    
    my $self = shift;
    
    $self->{dbh}->do(
        "create table if not exists simple_config (\n"
      . "    ID integer primary key autoincrement\n"
      . "  , key    text\n"
      . "  , value  text\n"
      . ")"
    );
    
}

sub get_auth_values {
    
    # This method is specific to the SQLite class. We don't store auth values in other databases
    
    my ( $self, $connection_name, $suppress_dialog ) = @_;
    
    my $auth_hash = $self->{dbh}->select(
        "select * from connections where ConnectionName = ?"
      , [ $connection_name ]
    );
    
    if ( ! $auth_hash && ! $suppress_dialog ) {
        
        window::dialog(
            undef
          , {
                title       => "Couldn't find connection in config database!"
              , type        => "error"
              , markup      => "The config manager was requested to build auth values for:\n\n"
                             . "Connection Name:  [<span color='blue'><b>$connection_name</b></span>]\n"
                             . " ... but there is no such entry in the Connections table.\n"
                             . "Please open the configuration screen and add such a connection.\n\n"
                             . "<i>Expect a bunch of errors after this message ...</i>"
            }
        );
        
        return undef;
        
    }
    
    return $auth_hash->[0];
    
}

sub get_db_type {
    
    # This method is specific to the SQLite class. We don't store auth values in other databases
    
    my ( $self , $connection_name ) = @_;
    
    my $type_hash = $self->{dbh}->select(
        "select DatabaseType from connections where ConnectionName = ?"
      , [ $connection_name ]
    );
    
    return $type_hash->[0]->{DatabaseType};
    
}

sub gui_overlay_names_and_types {
    
    # TODO: remove when new snapshotting complete
    
    my $self = shift;
    
    my $overlays = $self->{dbh}->select(
        "select OverlayName, OverlayPath from gui_overlays where Active = 1"
    );
    
    return $overlays;
    
}

sub all_gui_repositories {
    
    my $self = shift;
    
    my @repositories = keys %{$self->{gui_package_map}};
    
    push @repositories, 'core';
    
    return \@repositories;
    
}

sub repo_path {
    
    my ( $self , $type , $repo , $builtin_or_persisted ) = @_;

    if ( ! defined $builtin_or_persisted ) {
        $builtin_or_persisted = ( $self->{globals}->{self}->{flatpak} ? 'persisted' : 'builtin' );
    }

    if ( $builtin_or_persisted eq 'builtin' ) {
        if ( $repo eq 'core' ) {
            return $self->{globals}->{paths}->{app} . "/packages/";
        } else {
            return $self->{ $type . '_package_map' }->{$repo} . "/packages/";
        }
    } else {
        if ( $repo eq 'core' ) {
            return $ENV{"HOME"} . "/SDF_Persisted/packages/core/";
        } else {
            return $ENV{"HOME"} . "/SDF_Persisted/packages/";
        }
    }
    
}

sub db_class_path {

    my ( $self, $repo ) = @_;

    if ( $repo eq 'core' ) {
        return $self->{globals}->{paths}->{app} . "/Database/Connection/";
    } else {
        return $self->{ 'gui_package_map' }->{$repo} . "/Database/Connection/";
    }

}

sub all_database_drivers {

    my $self = shift;

    my $repos = $self->all_gui_repositories;

    my @all_database_drivers;
    
    my $ignore_drivers = $self->{dbh}->select(
        "select * from ignore_drivers"
      , []
      , "driver"
    );

    foreach my $repo ( @{$repos} ) {

        my $db_class_path = $self->db_class_path( $repo );

        my @files;

        finddepth(
            sub {
                return if($_ eq '.' || $_ eq '..');
                push @files, $File::Find::name;
            }
          , $db_class_path
        );

        foreach my $file ( @files ) {

            my $db_class_name;

            if ( $file =~ /.*Database\/Connection\/(.*)\.pm$/ ) {
                $db_class_name = $1;
                $db_class_name =~ s/\//::/g; # substitute slashes with ::
                if ( ! exists $ignore_drivers->{ $db_class_name } ) {
                    push @all_database_drivers, $db_class_name;
                }
            }

        }

    }

    my @sorted_dbs = sort( @all_database_drivers );

    return \@sorted_dbs;

}

sub all_database_types {
    
    my $self = shift;
    
    my $all_database_drivers = $self->all_database_drivers;
    
    my $all_types_hash;
    
    foreach my $driver ( @{$all_database_drivers} ) {
        $driver =~ s/::.*//g;
        $all_types_hash->{$driver} ++;
    }

    my @keys = keys %{$all_types_hash};
    my @sorted = sort( @keys );

    return \@sorted;
    
}

sub set_environment_variables {
    
    my $self = shift;
    
    my $env_vars = $self->{dbh}->select(
        "select key, value from simple_config where key like 'ENV:%'"
    );
    
    foreach my $env_record ( @{$env_vars} ) {
        
        my $env_key = $env_record->{key};
        my $value   = $env_record->{value};
        
        my $key;
        
        if ( $env_key =~ /ENV:([\w]*)/ ) {
            $key = $1;
            print "Setting environment variable: $key: $value\n";
            $ENV{ $key } = $value;
        } else {
            warn "Failed to parse ENV string: $env_key";
        }
    }
    
}

sub get_db_connection {
    
    my ( $self , $connection_name , $database_name ) = @_;
    
    my $key = $connection_name . ":" . $database_name;
    
    if ( ! exists $self->{db_connection_cache}->{ $key } ) {
        my $auth_hash  = $self->get_auth_values( $connection_name );
        $auth_hash->{Database} = $database_name;
        $self->{db_connection_cache}->{ $key } = Database::Connection::generate(
            $self->{globals}
          , $auth_hash
        );
    }
    
    return $self->{db_connection_cache}->{ $key };
    
}

1;