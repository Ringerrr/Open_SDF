package Database::Connection::Google::OAuth2;

use parent 'Database::Connection::Google';

use strict;
use warnings;

use Glib qw | TRUE FALSE |;

use constant DB_TYPE => 'GoogleOAuth2';

sub connection_label_map {

    my $self = shift;

    return {
        Username        => "Client ID"
      , Password        => "Refresh Token"
      , Database        => ""
      , Host_IP         => "Client Secret"
      , Port            => ""
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
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

        # Store auth credentials for later bucket access
        $self->auth_config({
            client_id       => $auth_hash->{Username},
            client_secret   => $auth_hash->{Host},
            refresh_token   => $auth_hash->{Password}
        });

        # Test connection by listing buckets (requires project-level permissions)
        # Note: Google::Cloud::Storage::Bucket is bucket-scoped, so we can't easily
        # test the connection without knowing a bucket name. We'll just store the config.

    };

    my $err = $@;

    if ( $err ) {
        $self->dialog(
            {
                title   => "Failed to configure GCS OAuth2 connection"
              , type    => "error"
              , text    => $err
            }
        );
        return undef;
    }

    return 1;

}

1;
