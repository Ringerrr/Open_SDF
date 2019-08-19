package window::odbc_config;

use warnings;
use strict;

use parent 'window';

use Glib qw( TRUE FALSE );

use File::Basename;
use File::Copy;
use File::Temp qw/ tempdir /;

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "odbc_config"
    );
    
    $self->{builder}->connect_signals( undef, $self );

    $self->{odbcinst_paths} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                     => $self->{globals}->{local_db}
          , column_sorting          => 1
          , sql                     => {
                                            select      => "*"
                                          , from        => "odbcinst_paths"
                                       }
          , fields                  => [
                                        {
                                            name        => "ID"
                                          , renderer    => "hidden"
                                        }
                                      , {
                                            name        => "Path"
                                          , x_percent   => 100
                                        }
                                       ]
          , vbox                    => $self->{builder}->get_object( 'odbcinst_paths_box' )
          , auto_tools_box          => TRUE
          , recordset_tool_items    => [ "detect" , "insert" , "delete" , "apply", "regen" ]
          , recordset_extra_tools   => {
                                            detect => {
                                                  type        => 'button'
                                                , markup      => "<span color='blue'>detect paths</span>"
                                                , icon_name   => 'edit-find'
                                                , coderef     => sub { $self->detect_paths }
                                            }
            }
        }
    );

    $self->{odbc_driver_options} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh            => $self->{globals}->{local_db}
          , read_only      => 1
          , column_sorting => 1
          , sql            => {
                                      select => "*"
                                    , from   => "odbc_driver_options"
                                    , where  => "0=1"
            }
          , fields         => [
                                    {
                                        name        => "ID"
                                      , renderer    => "hidden"
                                    }
                                    , {
                                        name       => "Driver"
                                      , renderer   => "hidden"
                                    }
                                    , {
                                        name        => "Option"
                                      , x_percent   => 50
                                    }
                                    , {
                                        name        => "Value"
                                      , x_percent   => 50
                                    }
            ]
            , vbox           => $self->{builder}->get_object( 'driver_options_box' )
        }
    );

    $self->{odbc_drivers} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                     => $self->{globals}->{local_db}
          , read_only               => 1
          , column_sorting          => 1
          , sql                     => {
                                        select          => "*"
                                      , from            => "odbc_drivers"
            }
          , fields                  => [
                                        {
                                            name        => "ID"
                                          , renderer    => "hidden"
                                        }
                                      , {
                                            name        => "DefinitionPath"
                                          , x_percent   => 50
                                        }
                                      , {
                                            name        => "Driver"
                                          , x_percent   => 50
                                        }
            ]
          , vbox                    => $self->{builder}->get_object( 'ODBC_drivers_box' )
          , auto_tools_box          => TRUE
          , on_row_select           => sub { $self->refresh_driver_options( @_ ) }
          , recordset_tool_items    => [ "regen" ]
          , recordset_extra_tools   => {
                regen => {
                    type        => 'button'
                  , markup      => "<span color='green'>regenerate drivers</span>"
                  , icon_name   => 'preferences-other'
                  , coderef     => sub { $self->parse_odbc_configs }
                }
            }
        }
    );

    return $self;
    
}

sub detect_paths {

    my $self = shift;

    my @candidate_paths;

    if ( $^O eq "linux" ) {

        # If we're running inside flatpak, drivers outside out container
        # can't be used by us - they need to be built inside the container, for the container

        if ( ! $self->{globals}->{self}->{flatpak} ) {
            push @candidate_paths , $ENV{'HOME'} . "/.odbcinst.ini";
            push @candidate_paths , '/etc/odbcinst.ini';
            push @candidate_paths , '/etc/unixODBC/odbcinst.ini';
            push @candidate_paths , '/usr/local/etc/odbcinst.ini';
            push @candidate_paths , '/usr/local/etc/unixODBC/odbcinst.ini';
        } else {
            print "Running under flatpak ... skipping host odbc config ...";
            push @candidate_paths , '/app/etc/odbcinst.ini';
        }

    } else {

        $self->dialog(
            {
                title       => "Sorry"
              , type        => "info"
              , text        => "ODBC configuration is only supported on Linux. Sorry - you're on your own here ..."
            }
        );

        return;

    }

    foreach my $path ( @candidate_paths ) {

        print "Checking [$path]\n";

        if ( -e $path ) {

            $self->{globals}->{local_db}->do(
                "insert into odbcinst_paths ( Path ) select ? where not exists ( select ID from odbcinst_paths where Path = ? )"
              , [ $path , $path ]
            );

        }

    }

    $self->{odbcinst_paths}->query;

}

sub parse_odbc_configs {

    my $self = shift;

    my $odbc_config_paths = $self->{globals}->{local_db}->all_odbc_config_paths;

    $self->{globals}->{local_db}->do(
        "delete from odbc_drivers"
    );

    $self->{globals}->{local_db}->do(
        "delete from odbc_driver_options"
    );

    foreach my $path ( @{$odbc_config_paths} ) {

        if ( !-e $path ) {
            next;
        }

        my $this_fh;

        open ( $this_fh, "<$path" )
            || die( $! );

        my $this_driver;

        while ( my $line = <$this_fh> ) {

            chomp( $line );

            if ( $line =~ /\[([\w\s]*)\]/ ) {

                $this_driver = $1;

                if ( $this_driver eq 'ODBC' || $this_driver eq 'ODBC Drivers' ) {

                    $this_driver = undef; # these aren't drivers

                } else {

                    $self->{globals}->{local_db}->do(
                        "insert into odbc_drivers ( DefinitionPath , Driver ) values ( ? , ? )"
                        , [ $path, $this_driver ]
                    );

                }

            } elsif ( $this_driver && $line =~ /([\w]*)\s*=\s*(.*)/ ) {

                $self->{globals}->{local_db}->do(
                    "insert into odbc_driver_options ( Driver , OptionName , OptionValue ) values ( ? , ? , ? )"
                    , [ $this_driver, $1, $2 ]
                );

            }

        }

        close $this_fh;

    }

    $self->{odbc_drivers}->query;

}

sub refresh_driver_options {

    my $self = shift;

    my $driver = $self->{odbc_drivers}->get_column_value( "Driver" );

    $self->{odbc_driver_options}->query(
        {
            where       => "Driver = ?"
          , bind_values => [ $driver ]
        }
    );

}

sub on_odbc_config_destroy {

    my $self = shift;

    $self->{globals}->{windows}->{'window::configuration'}->setup_odbc_driver_combo();

    $self->close_window();

}

1;
