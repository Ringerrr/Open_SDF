package window::dashboard;

use strict;

use parent 'window';

use Glib ( qw ' TRUE FALSE ' );

use Data::Dumper;

use constant    FLASH_TIMEOUT       => 500;
use constant    TOTAL_DATA_SLICES   => 120;

sub new {
    
    my ( $class, $globals ) = @_;
    
    my $self;
    $self->{globals} = $globals;
    bless $self, $class;
    
    $self->{flashers} = {};
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "dashboard"
    );
    
    $self->{builder}->connect_signals( undef, $self );
    
    $self->{progress} = $self->{builder}->get_object( "progress" );
    
    $self->{builder}->get_object( "dashboard" )->maximize;
    
    $self->{mem_dbh} = Database::Connection::SQLite->new(
        $self->{globals}
      , {
            location    => ":memory:"
        }
    );
    
    $self->{mem_dbh}->do( "PRAGMA default_synchronous = OFF" );
    $self->{mem_dbh}->do( "PRAGMA journal_mode = MEMORY" );
    
    my $auth_hash = $globals->{config_manager}->get_auth_values( "Netezza" );
    
    $auth_hash->{Database} = 'SA_GLOBALS';
    
    $self->{ENV}    = $globals->{config_manager}->simpleGet( "ENV" );
    
    $self->{globals_dbh} = Database::Connection::Netezza->new(
        $self->{globals}
      , $auth_hash
    );
    
#    $self->{globals_dbh} = $self->get_db_connection( "Netezza", "SA_GLOBALS" );
    
    my $sql = "select SAMPLE_DATETIME from SPACE_UTILISATION_DETAILS order by SAMPLE_DATETIME desc";
    
    $self->pulse( "utilisation_date_picker" );
    
    $self->{utilisation_date_picker} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{globals_dbh}
      , read_only               => 1
      , quick_renderers         => 1
      , sql                     => {
                                            pass_through    => "select SAMPLE_DATETIME from SPACE_UTILISATION_DETAILS"
                                                             . " group by SAMPLE_DATETIME"
                                                             . " order by SAMPLE_DATETIME desc"
                                   }
      , vbox                    => $self->{builder}->get_object( "space_utilisation_date_picker_box" )
      , on_row_select           => sub { $self->on_utilisation_date_picker_row_select( @_ ) }
    } );
    
    $self->{environments} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                     => $self->{globals_dbh}
          , primary_keys            => [ "ENVIRONMENT_ID" ]
          , auto_incrementing       => 0
          , sql                     => {
                                            select    => "*"
                                          , from      => "ENVIRONMENTS"
                                       }
          , fields                  => [
                                            {
                                                name            => "ENVIRONMENT_ID"
                                              , renderer        => "hidden"
                                              , x_absolute      => 100
                                              , sequence_sql    => "select next value for ENV_SEQ as NEXT_ENV_SEQ"
                                            }
                                          , {
                                                name            => "Environment"
                                              , x_percent       => 100
                                            }
            ]
          , vbox                    => $self->{builder}->get_object( "environment_box" )
          , auto_tools_box          => 1
        }
    );
        
    $self->pulse( "databases" );
    
    $self->{databases} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                     => $self->{globals_dbh}
          , primary_keys            => [ "DATABASE_ID" ]
          , auto_incrementing       => 0
          , sql                     => {
                                            select    => "*"
                                          , from      => "DATABASES"
                                       }
          , fields                  => [
                                            {
                                                name            => "DATABASE_ID"
                                              , renderer        => "hidden"
                                              , x_absolute      => 100
                                              , sequence_sql    => "select next value for DB_SEQ as NEXT_DB_SEQ"
                                            }
                                          , {
                                                name            => "Database"
                                              , x_percent       => 100
                                            }
            ]
          , vbox                    => $self->{builder}->get_object( "database_box" )
          , auto_tools_box          => 1
        }
    );
        
    $self->{progress}->set_fraction( 0 );
    
    $self->flash_items();
    
    return $self;
    
}

sub on_utilisation_date_picker_row_select {
    
    my $self = shift;
    
    my $datetime = $self->{utilisation_date_picker}->get_column_value( "SAMPLE_DATETIME" );
    
    # The process that captures stats just captures the database name. Here, we try to
    # split that out into environment + database
    
    # First, we check whether we need to do that or not ...
    
    my $unmatched = $self->{globals_dbh}->select(
        "select DB_NAME from SPACE_UTILISATION_DETAILS where environment = '' or environment is null group by DB_NAME"
      , undef
      , "DB_NAME"
    );
    
    if ( $unmatched ) {
        
        # Note: We sort by length because we want to match on the longest string first
        my $all_databases = $self->{globals_dbh}->select(
            "select DATABASE_NAME from databases order by length(DATABASE_NAME) desc"
          , undef
        );
        
        my $all_environments = $self->{globals_dbh}->select(
            "select ENVIRONMENT from environments"
          , undef
          , "ENVIRONMENT"
        );
        
        foreach my $unmatched_db ( keys %{$unmatched} ) {
            
            print "Processing $unmatched_db\n";
            
            my $match;
             
            foreach my $registered_db ( @{$all_databases} ) {
                
                my $registered_db_name = $registered_db->{DATABASE_NAME};
                
                if ( $unmatched_db =~ /(\w*)_$registered_db_name$/ ) {
                    
                    my $environment  = $1;
                    
                    $match = 1;
                    
                    my $filter = "where SAMPLE_DATETIME = '$datetime' and DB_NAME = '$unmatched_db'";
                    
                    my $sql = "update SPACE_UTILISATION_DETAILS set ENVIRONMENT = '$environment', DB_NAME = '$registered_db_name' $filter";
                    print $sql . "\n";
                    
                    $self->pulse( "[$environment] . [$registered_db_name]" );
                    
                    $self->{globals_dbh}->do(
                        $sql
                    );
                    
                    $sql = "insert into DB_ENV_MAPPINGS (  FULL_DB_NAME, ENVIRONMENT, DB_NAME ) values ( '$unmatched_db', '$environment', '$registered_db_name' )";
                    print $sql . "\n";
                    
                    $self->{globals_dbh}->do(
                        $sql
                    );
                    
                    last;
                    
                }
                
            }
            
            if ( ! $match ) {
                print "No match for $unmatched_db :(\n";
                $self->{globals_dbh}->do(
                    "update SPACE_UTILISATION_DETAILS set\n"
                  . "    ENVIRONMENT = 'Global'\n"
                  . "  , DB_NAME = '$unmatched_db'\n"
                  . "where SAMPLE_DATETIME = '$datetime' and DB_NAME = '$unmatched_db'"
                );
            }
            
        }
        
        $self->pulse( "grooming SPACE_UTILISATION_DETAILS ..." );
        $self->{globals_dbh}->do( "groom table SPACE_UTILISATION_DETAILS" );
        
        $self->pulse( "generating statistics on SPACE_UTILISATION_DETAILS ..." );
        $self->{globals_dbh}->do( "generate statistics on SPACE_UTILISATION_DETAILS" );
        
        $self->pulse( "" );
        $self->{progress}->set_fraction( 0 );
        $self->kick_gtk;
        
    }
    
    my $sql = "select\n"
            . "    ENVIRONMENT, DB_NAME, TABLE_NAME, sum(ALLOCATED_MBYTES) as SUM_ALLOCATED_MBYTES\n"
            . "from\n"
            . "            SPACE_UTILISATION_DETAILS\n"
            . "where\n"
            . "    SAMPLE_DATETIME = '$datetime'\n"
            . "group by\n"
            . "    ENVIRONMENT, DB_NAME, TABLE_NAME";
    
    my $sth = $self->{globals_dbh}->prepare( $sql )
        || return;
    
    $self->{globals_dbh}->execute( $sth )
        || return;
    
    $self->{globals_dbh}->sth_2_sqlite(
        $sth
      , [
            {
                name    => "environment"
              , type    => "text"
            }
          , {
                name    => "db_name"
              , type    => "text"
            }
          , {
                name    => "table_name"
              , type    => "text"
            }
          , {
                name    => "allocated_mbytes"
              , type    => "real"
            }
        ]
      , $self->{mem_dbh}
      , "space_utilisation"
      , $self->{builder}->get_object( "ProgressBar" )
    );
    
    if ( exists $self->{space_utilisation} ) {
        $self->{space_utilisation}->destroy;
    }
    
    $self->{space_utilisation} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                 => $self->{mem_dbh}
      , read_only           => 1
      , column_sorting      => 1
      , footer              => 1
      , sw_footer_no_scroll => 1
      , sql                 => {
                                  select    => "environment, db_name, table_name, allocated_mbytes"
                                , from      => "space_utilisation"
                               }
      , fields              => [
            {
                name        => "environment"
              , x_percent   => 25
            }
          , {
                name        => "db_name"
              , x_percent   => 25
            }
          , {
                name        => "table_name"
              , x_percent   => 25
            }
          , {
                name        => "allocated_mbytes"
              , x_percent   => 25
              , renderer    => "number"
              , number      => { separate_thousands  => TRUE } # this also activates numeric sorting
              , footer_function => "sum"
            }
        ]
      , vbox                => $self->{builder}->get_object( "space_utilisation_box" )
      , on_row_select       => sub { $self->on_space_utilisation_row_select( @_ ) }
    } );
    
}

sub on_filter_changed {
    
    my $self = shift;
    
    my $env_filter   = $self->{builder}->get_object( "Environment" )->get_text;
    my $db_filter    = $self->{builder}->get_object( "Database" )->get_text;
    my $table_filter = $self->{builder}->get_object( "Table" )->get_text;
    
    my @filters;
    
    if ( $env_filter ) {
        push @filters, "environment = '$env_filter'";
    }
    
    if ( $db_filter ) {
        push @filters, "db_name like '%$db_filter%'";
    }
    
    if ( $table_filter ) {
        push @filters, "table_name like '%$table_filter%'";
    }
    
    my $filter_string;
    
    if ( @filters ) {
        $filter_string = "where " . join( " and ", @filters );
    } else {
        $filter_string = "where 1=1";
    }
    
    $self->{space_utilisation}->query( $filter_string );
    
}

sub on_space_utilisation_row_select {
    
    my $self = shift;
    
    my $env   = $self->{space_utilisation}->get_column_value( "environment" );
    my $db    = $self->{space_utilisation}->get_column_value( "db_name" );
    my $table = $self->{space_utilisation}->get_column_value( "table_name" );
    my $sample_datetime = $self->{utilisation_date_picker}->get_column_value( 'SAMPLE_DATETIME' );
    
    my $sql = "select SPU_ID, DSID, ALLOCATED_MBYTES, 0 as DISTRIBUTION from SPACE_UTILISATION_DETAILS\n"
            . "where SAMPLE_DATETIME = '$sample_datetime'\n"
            . "  and ENVIRONMENT = '$env'\n"
            . "  and DB_NAME = '$db'\n"
            . "  and TABLE_NAME = '$table'";
    
    my $sth = $self->{globals_dbh}->prepare( $sql )
        || die( $self->{globals_dbh}->errstr );
    
    $sth->execute()
        || die( $sth->errstr );
    
    $self->{globals_dbh}->sth_2_sqlite(
        $sth
      , [
            {
                name    => "spu_id"
              , type    => "number"
            }
          , {
                name    => "dsid"
              , type    => "number"
            }
          , {
                name    => "allocated_mbytes"
              , type    => "real"
            }
          , {
                name    => "distribution"
              , type    => "real"
            }
        ]
      , $self->{mem_dbh}
      , "skew"
      , $self->{builder}->get_object( "ProgressBar" )
    );
    
    my $max_mbytes = $self->{mem_dbh}->select(
        "select max(allocated_mbytes) as max_mbytes from skew"
    );
    
    $sql = "update skew set distribution = allocated_mbytes / " . $max_mbytes->[0]->{max_mbytes} . " * 100";
    
    print "\n\n$sql\n\n";
    
    $self->{mem_dbh}->do( $sql );
    
    if ( exists $self->{skew} ) {
        my $sw = $self->{skew}->{treeview}->get_parent;
        $self->{skew}->{treeview}->destroy;
        $sw->destroy;
    }
    
    $self->{skew} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                 => $self->{mem_dbh}
      , read_only           => 1
      , column_sorting      => 1
      , sql                 => {
                                  select    => "spu_id, dsid, allocated_mbytes, distribution"
                                , from      => "skew"
                               }
      , fields              => [
            {
                name        => "spu_id"
              , x_absolute  => 120
              , renderer    => "number"
              , number      => {}
            }
          , {
                name        => "dsid"
              , x_absolute  => 120
              , renderer    => "number"
              , number      => {}
            }
          , {
                name        => "allocated_mbytes"
              , x_absolute  => 120
              , renderer    => "number"
              , number      => { separate_thousands  => TRUE }
            }
          , {
                name        => "distribution"
              , x_percent   => 100
              , renderer    => "progress"
              , number      => {}
            }
        ]
      , vbox                => $self->{builder}->get_object( "skew_box" )
    } );
    
    my $data_slices = $self->{skew}->count;
    
    $self->{builder}->get_object( "data_slices" )->set_text( $data_slices );
    
    if ( $data_slices < TOTAL_DATA_SLICES ) {
        $self->{flashers}->{data_slices}->{label} = $self->{builder}->get_object( "data_slices_lbl" );
        $self->{flashers}->{data_slices}->{flashing} = 1;
    } else {
        $self->{flashers}->{data_slices}->{label} = $self->{builder}->get_object( "data_slices_lbl" );
        $self->{flashers}->{data_slices}->{flashing} = 0;
    }
    
    # TODO: :)
    my $distribution_keys = $self->{globals_dbh}->select(
        "select ATTNAME from _v_table_dist_map where tablename = ?"
      , [ $table ]
    );
    
    my $key_count;
    
    foreach my $key_rec ( @{$distribution_keys} ) {
        
        $key_count ++;
        
        my $column_def = db::select(
            $self->{globals_dbh}
          , "select * from _v_relation_column where type = 'TABLE' and name = ?"
          , [ $table ]
        );
        
    }
    
}

#############
# Other stuff
#############

sub flash_items {
    
    # This code flashes labels and things based on flags that get set when checks are run
    
    my ( $self, $mode )  = @_;
    
    foreach my $flasher_name ( keys %{$self->{flashers}} ) {
        my $flasher = $self->{flashers}->{$flasher_name};
        if ( $mode ) {
            if ( $flasher->{flashing} ) {
                $flasher->{label}->set_markup( "<b><span color='red'>" . $flasher->{label}->get_text . "</span></b>" );
            }
        } else {
            $flasher->{label}->set_markup( "<b>" . $flasher->{label}->get_text . "</b>" );
        }
    }
    
    if ( $mode ) {
        # Queue the next action: flash OFF
        Glib::Timeout->add( FLASH_TIMEOUT, sub { $self->flash_items( FALSE ) } );
    } else {
        # Queue the next action: flash ON
        Glib::Timeout->add( FLASH_TIMEOUT, sub { $self->flash_items( TRUE ) } );
    }
    
    return FALSE;
    
}

1;
