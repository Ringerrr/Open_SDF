package Database::Connection::Google::ServiceAccount;

use parent 'Database::Connection::Google';

use strict;
use warnings;

use Google::Cloud::Storage::Bucket;

use Glib qw | TRUE FALSE |;


sub connection_label_map {

    my $self = shift;

    return {
        Username        => ""
      , Password        => ""
      , Database        => ""
      , Host_IP         => "Private Key File Path"
      , Port            => ""
      , Attribute_1     => "Bucket"
      , Attribute_2     => "Service Account Email"
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };

}

sub connection_browse_title {

    my $self = shift;

    return "Select a Google Service Account Private Key File";

}

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    eval {

        # Validate required fields
        unless ( $auth_hash->{Attribute_2} ) {
            die "Service Account Email (Attribute_2) is required";
        }

        unless ( $auth_hash->{Host} ) {
            die "Private Key File Path (Host) is required";
        }

        unless ( -e $auth_hash->{Host} ) {
            die "Private key file does not exist: " . $auth_hash->{Host};
        }

        # Store auth credentials for later bucket access
        $self->auth_config({
            client_email        => $auth_hash->{Attribute_2},
            private_key_file    => $auth_hash->{Host}
        });

        # Test connection by listing buckets (requires project-level permissions)
        # Note: Google::Cloud::Storage::Bucket is bucket-scoped, so we can't easily
        # test the connection without knowing a bucket name. We'll just store the config.

    };

    my $err = $@;

    if ( $err ) {
        $self->dialog(
            {
                title   => "Failed to configure GCS Service Account connection"
              , type    => "error"
              , text    => $err
            }
        );
        return undef;
    }

    return 1;

}

1;
