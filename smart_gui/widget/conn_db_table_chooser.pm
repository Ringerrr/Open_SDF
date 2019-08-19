package widget::conn_db_table_chooser;

use warnings;
use strict;

use Glib qw( TRUE FALSE );

use parent 'window';

sub new {
    
    my ( $class, $globals, $box, $control_hash, $callbacks , $options ) = @_;
    
    my $self;
    
    if ( ! $control_hash ) {
        $control_hash = {
            database    => 1
          , table       => 1
        };
    }
    
    $self->{globals}        = $globals;
    $self->{box}            = $box;
    $self->{control_hash}   = $control_hash;
    $self->{callbacks}      = $callbacks;
    
    bless $self, $class;
    
    # Create a hash of connection names to DB types. We use this to determine what class of connection
    # object to construct, based on a connection name
    my $sth = $self->{globals}->{local_db}->prepare(
        "select ConnectionName, DatabaseType from connections"
    );
    
    $sth->execute();
    
    $self->{connection_to_db_type_map} = $sth->fetchall_hashref( "ConnectionName" );
    
    # The Connection combo
    $self->{connection_combo} = Gtk3::ComboBox->new();
    
    my $model = Gtk3::ListStore->new( "Glib::String", "Gtk3::Gdk::Pixbuf" );

    my $sql = "select    ConnectionName\n"
            . "from      connections\n";

    if ( exists $options->{connection_filter} ) {
        $sql .= $options->{connection_filter} . "\n";
    }

    $sql .= "order by  ConnectionName";

    my $connections = $self->{globals}->{local_db}->select(
        $sql
      , [] # delete ?
    );
    
    foreach my $connection ( @{$connections} ) {
        
        my $icon_path = $self->get_db_icon_path( $self->{connection_to_db_type_map}->{ $connection->{ConnectionName} }->{DatabaseType} );
        my $icon;
        
        if ( $icon_path ) {
            $icon = $self->to_pixbuf( $icon_path );
        }
        
        $model->set(
            $model->append
          , 0, $connection->{ConnectionName}
          , 1, $icon
        );
        
    }
    
    # TODO:
    # The 'Add New Connection' entry
    
    $self->{connection_combo}->set_model( $model );
    $self->create_combo_renderers( $self->{connection_combo}, 0, 1 );
    
    $self->{connection_combo}->signal_connect( 'changed'  => sub { $self->on_connection_changed() } );

    if ( ! $options->{no_labels} ) {
        my $frame = Gtk3::Frame->new( "Connection" );
        $frame->add( $self->{connection_combo} );
        $self->{box}->pack_start( $frame , TRUE , TRUE , 0 );
    } else {
        $self->{box}->pack_start( $self->{connection_combo} , TRUE , TRUE , 0 );
    }

    if ( ! $control_hash->{database} ) {
        $self->{box}->show_all;
        return $self;
    }
    
    # The Database combo
    $self->{database_combo} = Gtk3::ComboBox->new();
    
    $model = Gtk3::ListStore->new( "Glib::String" );
    
    $self->{database_combo}->set_model( $model );
    $self->create_combo_renderers( $self->{database_combo}, 0 );
    
    $self->{database_combo}->signal_connect( 'changed'  => sub { $self->on_database_changed() } );

    if ( ! $options->{no_labels} ) {
        my $frame = Gtk3::Frame->new( "Database" );
        $frame->add( $self->{database_combo} );
        $self->{box}->pack_start( $frame , TRUE , TRUE , 0 );
    } else {
        $self->{box}->pack_start( $self->{database_combo} , TRUE , TRUE , 0 );
    }

    if ( ! $control_hash->{schema} && ! $control_hash->{table} ) {
        $self->{box}->show_all;
        return $self;
    }
    
    # The Schema combo
    $self->{schema_combo} = Gtk3::ComboBox->new();
    
    $model = Gtk3::ListStore->new( "Glib::String" );
    
    $self->{schema_combo}->set_model( $model );
    $self->create_combo_renderers( $self->{schema_combo}, 0 );
    
    $self->{schema_combo}->signal_connect( 'changed'  => sub { $self->on_schema_changed() } );

    if ( ! $options->{no_labels} ) {
        my $frame = Gtk3::Frame->new( "Schema" );
        $frame->add( $self->{schema_combo} );
        $self->{box}->pack_start( $frame , TRUE , TRUE , 0 );
    } else {
        $self->{box}->pack_start( $self->{schema_combo} , TRUE , TRUE , 0 );
    }

    if ( ! $control_hash->{table} ) {
        $self->{box}->show_all;
        return $self;
    }
    
    # The Table combo
    $self->{table_combo} = Gtk3::ComboBox->new();
    
    $model = Gtk3::ListStore->new( "Glib::String" );
    
    $self->{table_combo}->set_model( $model );
    $self->create_combo_renderers( $self->{table_combo}, 0 );
    
    $self->{table_combo}->signal_connect( 'changed'  => sub { $self->on_table_changed() } );

    if ( ! $options->{no_labels} ) {
        my $frame = Gtk3::Frame->new( "Table" );
        $frame->add( $self->{table_combo} );
        $self->{box}->pack_start( $frame , TRUE , TRUE , 0 );
    } else {
        $self->{box}->pack_start( $self->{table_combo} , TRUE , TRUE , 0 );
    }

    $self->{box}->show_all;
    
    return $self;
    
}

sub get_connection_name {
    
    my $self = shift;
    
    return $self->get_combo_value( $self->{connection_combo} );
    
}

sub set_connection_name {
    
    my ( $self, $connection_name ) = @_;
    
    return $self->set_combo_value( $self->{connection_combo}, $connection_name );
    
}

sub get_database_name {
    
    my $self = shift;
    
    return $self->get_combo_value( $self->{database_combo} );
    
}

sub set_database_name {
    
    my ( $self, $database_name ) = @_;
    
    return $self->set_combo_value( $self->{database_combo}, $database_name );
    
}

sub get_schema_name {
    
    my $self = shift;
    
    return $self->get_combo_value( $self->{schema_combo} );
    
}

sub set_schema_name {
    
    my ( $self, $schema_name ) = @_;
    
    return $self->set_combo_value( $self->{schema_combo}, $schema_name );
    
}

sub get_table_name {
    
    my $self = shift;
    
    return $self->get_combo_value( $self->{table_combo});
    
}

sub set_table_name {
    
    my ( $self, $table_name ) = @_;
    
    return $self->set_combo_value( $self->{table_combo}, $table_name );
    
}

sub get_db_type {
    
    my $self = shift;
    
    my $connection_name = $self->get_connection_name;
    
    if ( ! $connection_name ) {
        
        $self->dialog(
            {
                title       => "Missing connection name"
              , type        => "error"
              , text        => "widget::conn_db_table_chooser::get_db_type() called without a connection being selected!"
            }
        );
        
        return undef;
        
    }
    
    return $self->{globals}->{config_manager}->get_db_type( $connection_name );
    
}

sub get_db_connection {
    
    my ( $self, $connection_name, $database_name ) = @_;
    
    if ( ! $connection_name ) {
        $connection_name = $self->get_connection_name;
    }
    
    if ( $self->{control_hash}->{database} ) {
        if ( ! $database_name && ! $self->{_connection_changing} ) {
            $database_name = $self->get_database_name;
        }
    }

    my $key_name = $connection_name . '_' . ( defined $database_name ? $database_name : '' );
    
    if ( ! $self->{ $key_name } ) {
        
        my $auth_hash           = $self->{globals}->{config_manager}->get_auth_values( $connection_name );
        my $db_type             = $self->{globals}->{config_manager}->get_db_type( $connection_name );
        
        # More DB-specific hacks ...
        # For these DBs, we want to DESTORY the connection string, and inject our current database
        # This will trigger the connection class to reconstruct the connection string ( which includes
        # the database )
        
        # We need this. Eg it's needed to select the active database - see DB browser
        # But this also breaks some things - document what next time, and fix in a way
        # that doesn't break the DB browser - try executing some SQL against a non-default database
        if ( $database_name ) {
            $auth_hash->{Database} = $database_name;
        }
        
        if ( $db_type eq 'Netezza' || $db_type eq 'MySQL' || $db_type eq 'Postgres' || $db_type eq 'Greenplum' ) {
#        if ( $db_type eq 'Netezza' || $db_type eq 'MySQL' ) {
            delete $auth_hash->{ConnectionString};
            # $auth_hash->{Database} = $database_name;
        }
        
        #my $full_class = 'Database::Connection::' . $auth_hash->{DatabaseType};
        #
        #$self->{ $connection_name . '_' . $database_name } = $full_class->new(
        #    $self->{globals}
        #  , $auth_hash
        #);
        
        $self->{ $key_name } = Database::Connection::generate(
            $self->{globals}
          , $auth_hash
        );
        
    }
    
    return $self->{ $key_name };
    
}

sub on_connection_changed {
    
    my $self            = shift;
    
    if ( $self->{database_combo} ) {

        $self->{_connection_changing} = 1;

        my $connection_name = $self->get_connection_name;
        my $db_type         = $self->{globals}->{config_manager}->get_db_type( $connection_name );
        
        my $database_name;
        
        # Not *quite* complicated enough to break out into separate classes ... but we need various hacks for different DBs ...
        if ( $db_type eq 'Netezza' ) {
            $database_name = 'SYSTEM';
        }
        
        my $connection = $self->get_db_connection(
            $connection_name
          , $database_name
        ) || return;
        
        my @databases = $connection->fetch_database_list();
        
        my $model = Gtk3::ListStore->new(
            "Glib::String"
        );
        
        foreach my $database ( @databases ) {
            $model->set(
                $model->append
              , 0 , $database
            );
        }
        
        $self->{database_combo}->set_model( $model );
        
        if ( $db_type eq 'Oracle' ) {
            
            # For Oracle, we want to hard-code the 'Database' to the username
            my $auth_hash = $self->{globals}->{config_manager}->get_auth_values( $connection_name );
            
            $self->set_combo_value( $self->{database_combo}, uc( $auth_hash->{Username} ) );
            $self->{database_combo}->set_sensitive( FALSE );
            
        } else {
            
            # For everything else, we make sure the combo is sensitive again ...
            $self->{database_combo}->set_sensitive( TRUE );
            
        }
        
        # The 'Add New Database' entry
        $model->set(
            $model->append
          , 0, '+ Add New Database'
        );
        
#        $connection->disconnect;

        $self->{_connection_changing} = 0;

    }
    
    if ( $self->{callbacks} && exists $self->{callbacks}->{on_connection_changed} ) {
    $self->{callbacks}->{on_connection_changed}();
    }
    
}

sub on_database_changed {
    
    my $self = shift;
    
    my $connection_name = $self->get_connection_name;
    my $database_name   = $self->get_database_name;
    
    if ( $database_name ne '+ Add New Database' ) {
        
        my $connection = $self->get_db_connection(
            $connection_name
          , $database_name
        ) || return;
        
        if ( $connection->has_schemas ) {
            
            if ( $self->{control_hash}->{schema} ) {
                $self->populate_schemas_combo;
            }
            
        } else {
            
            if ( $self->{control_hash}->{table} ) {
                $self->populate_tables_combo;
            }
            
        }
        
        if ( $self->{callbacks} && exists $self->{callbacks}->{on_database_changed} ) {
            $self->{callbacks}->{on_database_changed}();
        }
        
    } else {
        
        my $new_database = $self->dialog(
            {
                title       => "New Database Name?"
              , type        => "input"
            }
        );
        
        if ( ! $new_database ) {
            return;
        }
        
        my $connection = $self->get_db_connection(
            $connection_name
        ) || return;
        
        $connection->do(
            "create database $new_database"
        );
        
        $self->on_connection_changed;
        
        $self->set_widget_value( $self->{database_combo}, $new_database );
        
    }
    
}

sub on_schema_changed {
    
    my $self = shift;
    
    my $connection_name = $self->get_connection_name;
    my $database_name   = $self->get_database_name;
    
    my $connection = $self->get_db_connection(
        $connection_name
      , $database_name
    ) || return;
    
    if ( $self->get_schema_name ne '+ Add New Schema' ) {
        
        if ( $self->{control_hash}->{table} ) {
            $self->populate_tables_combo;
        }
        
        if ( $self->{callbacks} && exists $self->{callbacks}->{on_schema_changed} ) {
            $self->{callbacks}->{on_schema_changed}();
        }
        
    } else {
        
        my $new_schema = $self->dialog(
            {
                title       => "New Schema Name?"
              , type        => "input"
            }
        );
        
        if ( ! $new_schema ) {
            return;
        }
        
        my $connection = $self->get_db_connection(
            $connection_name
          , $database_name
        ) || return;
        
        $connection->do(
            "create schema " . $connection->db_schema_string( $database_name, $new_schema )
        );
        
        $self->on_database_changed;
        
        $self->set_widget_value( $self->{schema_combo}, $new_schema );
        
    }
    
#    $connection->disconnect;
    
}

sub on_table_changed {
    
    my $self = shift;
    
    if ( $self->{callbacks} && exists $self->{callbacks}->{on_table_changed} ) {
        $self->{callbacks}->{on_table_changed}();
    }
    
}

sub populate_schemas_combo {
    
    my $self = shift;
    
    my $connection_name = $self->get_connection_name;
    my $database_name   = $self->get_database_name;
    
    my $connection = $self->get_db_connection(
        $connection_name
      , $database_name
    ) || return;
    
    my @schemas = $connection->fetch_schema_list( $database_name );
    
    my $model = Gtk3::ListStore->new(
        "Glib::String"
    );
    
    foreach my $schema ( @schemas ) {
        $model->set(
            $model->append
          , 0 , $schema
        );
    }
    
    # The 'Add New Schema' entry
    $model->set(
        $model->append
      , 0, '+ Add New Schema'
    );
    
    $self->{schema_combo}->set_model( $model );
    
#    $connection->disconnect;
    
}

sub populate_tables_combo {
    
    my $self = shift;
    
    my $connection_name = $self->get_connection_name;
    my $database_name   = $self->get_database_name;
    my $schema_name     = $self->get_schema_name;
    
    my $connection = $self->get_db_connection(
        $connection_name
      , $database_name
    ) || return;
    
    my @tables = $connection->fetch_table_list( $database_name, $schema_name );
    
    my $model = Gtk3::ListStore->new(
        "Glib::String"
    );
    
    foreach my $table ( @tables ) {
        $model->set(
            $model->append
          , 0 , $table
        );
    }
    
    $self->{table_combo}->set_model( $model );
    
#    $connection->disconnect;
    
}

1;
