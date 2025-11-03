package Database::Connection::Google::ServiceAccount;

use parent 'Database::Connection::Google';

use strict;
use warnings;

use Glib qw | TRUE FALSE |;

use constant DB_TYPE => 'GoogleServiceAccount';

sub connection_label_map {

    my $self = shift;

    return {
        Username        => ""
      , Password        => ""
      , Database        => ""
      , Host_IP         => ""
      , Port            => ""
      , Attribute_1     => ""
      , Attribute_2     => "Service Account Email"
      , Attribute_3     => "Private Key File Path"
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };

}

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    eval {

        require lib;
        lib->import('/Users/dankasak/src/perl_gcs');
        require Google::Cloud::Storage::Bucket;

        # Validate required fields
        unless ( $auth_hash->{Attribute_2} ) {
            die "Service Account Email (Attribute_2) is required";
        }

        unless ( $auth_hash->{Attribute_3} ) {
            die "Private Key File Path (Attribute_3) is required";
        }

        unless ( -e $auth_hash->{Attribute_3} ) {
            die "Private key file does not exist: " . $auth_hash->{Attribute_3};
        }

        # Store auth credentials for later bucket access
        $self->auth_config({
            client_email        => $auth_hash->{Attribute_2},
            private_key_file    => $auth_hash->{Attribute_3}
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
