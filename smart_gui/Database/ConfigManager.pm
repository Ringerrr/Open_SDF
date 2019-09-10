package Database::ConfigManager;

use strict;
use warnings;

use parent 'window'; # so we can create dialogs

use File::Path qw(make_path);
use Glib qw( TRUE FALSE );

sub new {
    
    my ( $class, $globals, $dbh, $config_type ) = @_;
    
    my $self;
    
    $self->{globals}        = $globals;
    $self->{dbh}            = $dbh;
    $self->{config_type}    = $config_type;
    
    bless $self, $class;
    
    $self->create_simple_config;
    
    $self->{exists_simple_config} = $dbh->prepare(
        "select key, value from simple_config where key = ?"
    );
    
    $self->{update_simple_config} = $dbh->prepare(
        "update simple_config set value = ? where key = ?"
    );
    
    $self->{insert_simple_config} = $dbh->prepare(
        "insert into simple_config ( key, value ) values ( ?, ? )"
    );
    
    my @overlay_types = ( 'core' );
    my @overlay_paths = $self->{globals}->{local_db}->gui_overlay_paths;
    
    if ( @overlay_paths ) {
        # This won't exist for the local config db
        push @overlay_types, @overlay_paths;
    }
    
    if ( ! $self->{globals}->{behaviour}->{skip_upgrades} ) {
        
        foreach my $overlay_type ( @overlay_types ) {
            $self->upgrade_schema( $overlay_type );
        }
        
    }
    
    return $self;
    
}

sub create_simple_config {
    
    my $self = shift;
    
    die( "Subclasses must implement create_simple_config()" );
    
}

sub simpleSet {
    
    my ( $self, $key, $value ) = @_;
    
    $self->{exists_simple_config}->execute( $key );
    
    my $exists = $self->{exists_simple_config}->fetchrow_hashref( 'NAME_lc' );
    
    if ( $exists ) {
        
        $self->{update_simple_config}->execute( $value, $exists->{key} );
        
    } else {
        
        $self->{insert_simple_config}->execute( $key, $value );
        
    }
    
}

sub simpleGet {
    
    my ( $self, $key ) = @_;
    
    $self->{exists_simple_config}->execute( $key );
    
    my $exists = $self->{exists_simple_config}->fetchrow_hashref( 'NAME_lc' );
    
    if ( $exists ) {
        
        return $exists->{value};
        
    } else {
        
        return undef;
        
    }
    
}

sub overlay_name_to_path {

    my ( $self , $name , $type ) = @_;

    # $type is gui or etl
    my $overlay_record = $self->{dbh}->select(
        "select OverlayPath from " . $type . "_overlays where OverlayName = ?"
      , [ $name ]
    );

    if ( @{$overlay_record} ) {
        return $overlay_record->[0]->{OverlayPath};
    }

}

sub schema_path {
    
    my ( $self, $type ) = @_;
    
    # $type is 'core', or an overlay path
    
    if ( ! $type || $type eq 'core' ) {
        return $self->{globals}->{paths}->{app} . "schemas/" . $self->TYPE .  "/" . $self->{config_type};
    } else {
        return $type . "/schemas/" . $self->TYPE . "/" . $self->{config_type};
    }
    
}

sub metadata_path {
    
    my ( $self, $type ) = @_;
    
    # $type is 'core', or an overlay path
    
    # This is used for old-style scripts that window/main.pm generated for snapshotting templates and configs.
    # It used to create a 'delete from range' DML, followed by a set of CSVs.
    
    # It is now also used to store our .package metadata files, that point to particular packages ( package_path() method )
    
    if ( ! $type || $type eq 'core' ) {
        return $self->{globals}->{paths}->{app} . "schemas/metadata";
    } else {
        return $type . "/schemas/metadata";
    }
    
}

sub package_path {
    
    my ( $self, $repository, $snapshot_type ) = @_;
    
    # $type is 'core', or an overlay path
    
    # This is the top-level directory of the new-style packages that contain JSON-encoded structures of an entire
    # template ( template record and associate parameters ) or config ( config, param_value, harvest, processing_group records )
    
    if ( ! $repository || $repository eq 'core' ) {
        return $self->{globals}->{paths}->{app} . "packages/" . $snapshot_type . "/";
    } else {
        return $repository . "packages/" . $snapshot_type . "/";
    }
    
}

sub template_icon_path {

    my ( $self , $repository ) = @_;

    # $type is 'core', or an overlay path

    if ( ! $repository || $repository eq 'core' ) {
        return $self->{globals}->{paths}->{app} . "icons/templates/";
    } else {
        return $self->overlay_name_to_path( $repository , 'gui' ) . "/icons/templates/";
    }

}

sub get_next_control_sequence {
    
    my ( $self, $type ) = @_;
    
    # $type is 'core', or an overlay path
    
    # This method looks in the 'schema' folder for the current control connection type,
    # and parses each filename to find the max sequence. It returns this value + 1.
    
    my $max_sequence = 0;
    
    my $schema_path = $self->metadata_path( $type );
    
    if ( ! -e $schema_path ) {
        
        eval {
            make_path( $schema_path )
                || die( $! );
        };
        
        my $err = $@;
        
        if ( $err ) {
            window::dialog(
                undef
              , {
                    title       => "Failed to create directory"
                  , type        => "error"
                  , text        => $err
                }
            );
            die( $err ); # this will halt further processing ( ie attempting to export anything ), but won't kill the app ...
        }
        
        
    }
    
    opendir( DIR, $schema_path ) || die( $! );
    
    while ( my $file = readdir(DIR) ) {
        
        my ( $sequence, $name, $extension );
        if ( $file =~ /(\d*)_([\w-]*)\.(dml|ddl|csv|package)$/i ) {
            ( $sequence, $name, $extension ) = ( $1, $2 );
        }
        
        if ( $sequence > $max_sequence ) {
            $max_sequence = $sequence;
        }
        
    }
    
    return $max_sequence + 1;
    
}

sub upgrade_schema {
    
    my ( $self, $type ) = @_;
    
    # First we do the actual schema scripts, which are generally DDLs
    # These are DB-specific
    
    my $version_key;
    
    if ( ! $type || $type eq 'core' ) {
        
        $version_key = "version:" . $self->{config_type};
        
    } else {
        
        # For the overlay name, we don't want the leading part - people
        # can connection to environments from multiple places, and the leading
        # parts of the overlay path will confuse things
        
        my $schema_key_path = $type;
        
        if ( $type =~ /.*\/([\w-]*)/ ) {
            $schema_key_path = $1;
        }
        
        $version_key = "version:" . $self->{config_type} . ":" . $schema_key_path;
        
    }
    
    my $version = $self->simpleGet( $version_key );
    
    my $schema_path = $self->schema_path( $type );
    
    $self->do_upgrades(
        {
            upgrade_path    => $schema_path
          , current_version => $version
          , version_key     => $version_key
        }
    );
    
    # Return if we're not a 'control' type ...
    
    if ( $self->{config_type} ne 'control' ) {
        return;
    }
    
    # ... or for control types ... we need to do metadata as well ...
    
    if ( ! $type || $type eq 'core' ) {
        
        $version_key = "version:metadata";
        
    } else {
        
        # For the overlay name, we don't want the leading part - people
        # can connection to environments from multiple places, and the leading
        # parts of the overlay path will confuse things
        
        my $schema_key_path = $type;
        
        if ( $type =~ /.*\/([\w-]*)/ ) {
            $schema_key_path = $1;
        }
        
        $version_key = "version:" . $self->{config_type} . ":" . $schema_key_path;
        
    }
    
    $version = $self->simpleGet( $version_key );
    
    my $metadata_path = $self->metadata_path( $type );
    
    $self->do_upgrades(
        {
            upgrade_path    => $metadata_path
          , current_version => $version
          , version_key     => $version_key
        }
    );

    print "\n";

}

sub do_upgrades {
    
    my ( $self, $options ) = @_;
    
    {
        no warnings 'uninitialized';
        print STDOUT "ConfigManager checking for upgrades ... version_key: [" . $options->{version_key} . "] ... version: [" . $options->{current_version} . "]\n";
    }
    
    my $upgrade_hash = {};
    
    if ( ! -d $options->{upgrade_path} ) {
        return;
    }
    
    opendir( DIR, $options->{upgrade_path} ) || warn( $! );
    
    while ( my $file = readdir(DIR) ) {
        
        if ( $file =~ /(\d*)_([\w-]*)\.(dml|ddl|csv)$/i ) {
            my ( $sequence, $name, $extension ) = ( $1, $2 );
            $upgrade_hash->{$sequence} = $file;
        }

    }
    
    close DIR;
    
    my $progress_bar = $self->{dbh}->{progress_bar};
    
    #eval {
        
        foreach my $sequence ( sort { $a <=> $b } keys %{$upgrade_hash} ) {
    
            if ( ! defined $options->{current_version} || $sequence > $options->{current_version} ) {
                
                my $this_file = $options->{upgrade_path} . "/" . $upgrade_hash->{$sequence};
                
                if ( $this_file =~ /.*\.csv$/ ) {
                    
                    my $target_table;
                    
                    if ( $this_file =~ /(\d*)_([\w-]*)_-_([\w-]*)\.csv$/i ) {
                        $target_table = $2;
                    } else {
                        
                        $self->dialog(
                            {
                                title       => "Error parsing CSV dump file name"
                              , type        => "error"
                              , text        => "File name: [$this_file]"
                            }
                        );
                        
                        return FALSE;
                        
                    }
                    
                    my $options = $self->{dbh}->generate_db_load_command(
                        {
                            file_path           => $this_file
                          , remote_client       => TRUE
                          , null_value          => '\N'
                          , delimiter           => ","
                          , skip_rows           => 1
                          , quote_char          => '"'
                          , encoding            => "latin9"     # TODO: "utf8" - causes issues with netezza char/varchar
                          , date_style          => "YMD"
                          , date_delim          => "-"
                          , database            => undef
                          , schema              => undef
                          , table               => $target_table
                          , escape_char         => "\\"
                        }
                    );
                    
                    $self->{dbh}->load_csv(
                        {
                            mem_dbh             => undef
                          , target_db           => undef
                          , target_schema       => undef
                          , target_table        => $target_table
                          , table_definition    => undef
                          , copy_command        => $options
                          , remote_client       => TRUE
                          , file_path           => $this_file
                          , progress_bar        => $progress_bar
                          , suppress_dialog     => TRUE
                        }
                    ) || return;
                    
                } else {
                    
                    # DDLs and DMLs get executed
                    
                    local $/;
                    
                    my $this_fh;
                    
                    open ( $this_fh, "<$this_file" )
                        || die( $! );
                    
                    my $contents = <$this_fh>;
                    
                    close $this_fh;
                    
                    if ( $progress_bar ) {
                        $progress_bar->set_text( "Upgrade schema executing: [" . $this_file . "]" );
                        $progress_bar->pulse;
                        $self->kick_gtk;
                    }
                    
                    eval {
                        $self->{dbh}->do( $contents )
                            || die( "Error upgrading schema:\n" . $self->{dbh}->errstr );
                    };
                    
                    my $err = $@;
                    
                    if ( $err ) {
                        
                        if ( $self->dialog(
                                 {
                                     title       => "Force DDL as 'executed'"
                                   , type        => "question"
                                   , text        => "The current DDL has failed to execute. Would you like to pretend it executed successfully and continue upgrading?"
                                 }
                             ) ne 'yes'
                        ) {
                            return;
                        }
                        
                    }
                    
                }
                
                $self->simpleSet( $options->{version_key}, $sequence );
                
            }
            
        }
        
    #};
    
    if ( $progress_bar ) {
        $progress_bar->set_text( "" );
        $progress_bar->set_fraction( 0 );
        $self->kick_gtk;
    }

}

sub sdf_connection {

    my ( $self , $connection_name ) = @_;

    # This is a convenience method for creating / returning our own connections.
    # Sometimes we won't have a CONTROL / LOG connection configured yet, so we can't assume we can always create
    # these connections on startup. We get called by places that *require* these connections.

    my $database_name;

    if ( $connection_name eq 'CONTROL' ) {

        $database_name = $self->{globals}->{CONTROL_DB_NAME};

    } elsif ( $connection_name eq 'LOG' ) {

        $database_name = $self->{globals}->{LOG_DB_NAME};

    } else {

        $self->dialog(
            {
                title      => "Bad connection name"
              , type       => "error"
              , text       => "Database::ConfigManager was asked to create a connection named [$connection_name],"
                            . " but this behaviour is not implemented. This is probably a fatal error"
            }
        );

        # This ( die ) gets caught in the main etl window ... not sure about other places:
        die( "Database::ConfigManager was asked to create a connection named [$connection_name],"
            . " but this behaviour is not implemented. This is probably a fatal error" );

    }

    if ( exists $self->{globals}->{connections}->{ $connection_name } ) {
        return $self->{globals}->{connections}->{ $connection_name };
    }

    my $values_hash = $self->{globals}->{config_manager}->get_auth_values( "METADATA" );

    # We're removing the connection string here because it includes the database,
    # and we want to specify it ourself ...

    delete $values_hash->{ConnectionString};
    $values_hash->{Database} = $database_name;

    eval {
        $self->{globals}->{connections}->{ $connection_name } = Database::Connection::generate(
            $self->{globals}
          , $values_hash
          , undef
          , lc( $connection_name )
        );
    };

    my $err = $@;

    if ( $err ) {

        print "\n\n$err";

        $self->dialog(
            {
                title       => "Couldn't create the minimum required connections"
              , type        => "error"
              , text        => "Please check your database connectivity for the [$connection_name] connection"
            }
        );

        $self->open_window( 'window::configuration' , $self->{globals} );

        return undef;

    }

    return $self->{globals}->{connections}->{ $connection_name };

}

sub kick_gtk {

    my $self = shift;

    if ( $self->{globals}->{self}->{broadway} ) {
        return;
    }

    Gtk3::main_iteration() while ( Gtk3::events_pending() );

}

1;
