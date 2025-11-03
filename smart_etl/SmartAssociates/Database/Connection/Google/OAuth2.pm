package SmartAssociates::Database::Connection::Google::OAuth2;

use strict;
use warnings;

use lib '/Users/dankasak/src/perl_gcs';
use Google::Cloud::Storage::Bucket;

use base 'SmartAssociates::Database::Connection::Google';

use constant DB_TYPE => 'GoogleOAuth2';

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    # Store auth credentials for later bucket access
    $self->auth_config({
        client_id       => $auth_hash->{Username},
        client_secret   => $auth_hash->{Host},
        refresh_token   => $auth_hash->{Password}
    });

    $self->dbh( {} );

}

sub _get_bucket_client {

    my ( $self, $bucket_name ) = @_;

    # Return cached bucket client if available
    my $cache = $self->bucket_cache();
    if ( exists $cache->{ $bucket_name } ) {
        return $cache->{ $bucket_name };
    }

    my $auth_config = $self->auth_config();

    my $bucket = Google::Cloud::Storage::Bucket->new({
        client_id       => $auth_config->{client_id},
        client_secret   => $auth_config->{client_secret},
        refresh_token   => $auth_config->{refresh_token},
        bucket_name     => $bucket_name
    });

    # Cache the bucket client
    $cache->{ $bucket_name } = $bucket;

    return $bucket;

}

1;
