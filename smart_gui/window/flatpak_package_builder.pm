package window::flatpak_package_builder;

use parent 'window';

use strict;
use warnings;

use JSON;
use LWP;
use LWP::UserAgent;
use HTML::Tree;
use Crypt::Digest::SHA256 qw | sha256 sha256_hex |;

use Glib qw( TRUE FALSE );

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "flatpak_package_builder"
    );

    $self->{mem_dbh} = Database::Connection::SQLite->new(
        $self->{globals}
      , {
            location    => ':memory:'
        }
    );

    $self->{mem_dbh}->do( "PRAGMA default_synchronous = OFF" );

    # this creates all the tables we use to store the schema metadata ...
    #  ... the db browser shares this code

    $self->{mem_dbh}->create_model_schema;

    # this create migration-specific tables
    $self->{mem_dbh}->do(
        "create table dependencies (\n"
      . "    ID                    integer       primary key\n"
      . "  , dependency            text\n"
      . ")" ) || return;

    $self->{deps_datasheet} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                 => $self->{mem_dbh}
      , sql                 => {
                                  select        => "dependency"
                                , from          => "dependencies"
                                , order_by      => "ID"
                               }
      , column_sorting      => 1
      , fields              => [
            {
                name        => "dependency"
              , x_percent   => 100
              , read_only   => TRUE
            }
        ]
      , vbox                => $self->{builder}->get_object( "package_dependencies_box" )
      , on_changed          => sub { $self->{source_db_schemas}->apply }
    } );

    $self->{deps_datasheet }->{treeview}->signal_connect(
        button_press_event => sub { $self->on_deps_datasheet_click( @_ ) }
    );

    $self->maximize;

    $self->manage_widget_value( 'flatpak_sources_directory' );

    $self->{builder}->connect_signals( undef, $self );
    
    return $self;
    
}

sub on_deps_datasheet_click {

    my ( $self , $widget , $event , $type ) = @_;

    if ( $event->type ne '2button-press' ) {
        return;
    }

    my $dep_url = $self->{deps_datasheet}->get_column_value( 'dependency' );

    $self->set_widget_value( 'metacpan_url' , $dep_url );
    $self->on_FetchMetacpan_clicked();

}

sub on_FetchMetacpan_clicked {

    my $self = shift;

    my $metacpan_url = $self->get_widget_value( 'metacpan_url' );

    if ( ! $metacpan_url ) {
        return;
    }
    
    if ( $metacpan_url !~ /^http/ ) {
        $metacpan_url = "https://metacpan.org/pod/$metacpan_url";
    }
    
    my $ua = LWP::UserAgent->new();

    my $request = HTTP::Request->new( GET => $metacpan_url );
    my $response = $ua->request( $request );
    my $content = $response->content();

    my $tree = HTML::Tree->new();
    $tree->parse( $content );

    my @elements = $tree->content_list();

    $self->{download_url} = undef;
    $self->{'deps-content-array'} = undef;
    $self->{'deps-array'} = undef;
    $self->{'deps-hash'} = undef;

    $self->scan_all_elements( 'itemprop' , 'downloadUrl' , 'download_url' , 'href' , 'scalar' , \@elements );

    $self->scan_all_elements( 'class' , 'nav-list box-right hidden-phone dependencies' , 'deps-content-array' , '_content' , 'array' , \@elements );

    $self->scan_all_elements( 'class' , 'ellipsis' , 'deps-array' , 'href' , 'array' , $self->{'deps-content-array'}->[0] );

    $self->{mem_dbh}->do( "delete from dependencies" );

    foreach my $dep_url ( @{ $self->{'deps-array'} } ) {
        if ( ! exists $self->{'deps-hash'}->{$dep_url} ) {
            $self->{mem_dbh}->do( "insert into dependencies ( dependency ) values ( ? )" , [ 'https://metacpan.org' . $dep_url ] );
            $self->{'deps-hash'}->{$dep_url} = 1;
        }
    }

    $self->{deps_datasheet}->query();

    if ( ! $self->{download_url} ) {

        $self->dialog(
            {
                title   => "Couldn't find download URL"
              , type    => "error"
              , text    => "I couldn't find a package download URL in the Metacpan page"
            }
        );

        return FALSE;

    }

    $self->set_widget_value( 'latest_package_url' , $self->{download_url} );

    $request = HTTP::Request->new( GET => $self->{download_url} );
    $response = $ua->request( $request );
    $content = $response->content();

    my $sha256 = sha256_hex( $content );
    $self->set_widget_value( 'sha256' , $sha256 );

    $self->{package_name};

    if ( $metacpan_url =~ /.*\/(.*)/ ) {
        $self->{package_name} = 'perl-' . $1;
        $self->{package_name} =~ s/::/-/g;
    }

    my $package_structure = {
        name        => $self->{package_name}
      , cleanup     => [ "/bin" ]
      , "no-autogen"=> JSON::true
      , sources     => [
                           {
                                type             => "archive"
                              , url              => $self->{download_url}
                              , sha256           => $sha256
                           }
                         , {
                                type             => "file"
                              , path             => "perl-MakefilePL-Makefile"
                              , "dest-filename"  => "Makefile"
                           }
                       ]
    };

    my $package_json = to_json( $package_structure , { pretty => 1 } );

    $self->set_widget_value( 'package_json' , $package_json );

}

sub scan_all_elements {

    my ( $self , $lookdown_a , $lookdown_b , $store_key , $element_key , $store_action , $elements ) = @_;

    foreach my $element ( @{$elements} ) {
        if ( ref $element eq 'HTML::Element' ) {
            my $a_element = $element->look_down( $lookdown_a , $lookdown_b );
            if ( $a_element ) {
                if ( exists $a_element->{ $element_key } ) {
                    if ( $store_action eq 'scalar' ) {
                        $self->{ $store_key } = $a_element->{ $element_key };
                        return;
                    } elsif ( $store_action eq 'array' ) {
                        push @{ $self->{ $store_key } } , $a_element->{ $element_key };
                    }
                }
            }
            my @more_elements = $element->content_list();
            $self->scan_all_elements( $lookdown_a , $lookdown_b , $store_key , $element_key , $store_action , \@more_elements );
        }
    }

}

sub on_Browse_clicked {

    my $self = shift;

    my $file = $self->file_chooser(
        {
            title   => "Please select the file to import ..."
          , action  => 'open'
          , type    => 'directory'
          , path    => $self->get_widget_value( 'flatpak_sources_directory' )
        }
    ) || return;

    $self->set_widget_value( 'flatpak_sources_directory' , $file );

}

sub on_write_package_file_clicked {

    my $self = shift;

    my $target_file_path = $self->get_widget_value( 'flatpak_sources_directory' ) . "/" . $self->{package_name} . ".json";

    if ( -e $target_file_path ) {
        my $answer = $self->dialog(
            {
                title       => "Overwrite file?"
              , type        => "question"
              , text        => "Target file [$target_file_path] already exists. Overwrite?"
            }
        );
        if ( $answer ne 'yes' ) {
            return FALSE;
        }
    }

    eval {

        open my $fh , ">$target_file_path"
            || die( $! );

        print $fh $self->get_widget_value( 'package_json' );

        close $fh
            || die( $! );

    };

    my $err = $@;

    if ( $err ) {
        $self->dialog(
            {
                title       => "Write error!"
              , type        => "error"
              , text        => $err
            }
        );
    }

    return FALSE;

}

sub on_flatpak_package_builder_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
