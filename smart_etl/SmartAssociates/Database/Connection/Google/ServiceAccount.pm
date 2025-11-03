package SmartAssociates::Database::Connection::Google::ServiceAccount;

use strict;
use warnings;

use lib '/Users/dankasak/src/perl_gcs';
use Google::Cloud::Storage::Bucket;

use base 'SmartAssociates::Database::Connection::Google';

use constant DB_TYPE => 'GoogleServiceAccount';

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    # Store auth credentials for later bucket access
    $self->auth_config({
        client_email        => $auth_hash->{Attribute_2},
        private_key_file    => $auth_hash->{Attribute_3}
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
        client_email        => $auth_config->{client_email},
        private_key_file    => $auth_config->{private_key_file},
        bucket_name         => $bucket_name
    });

    # Cache the bucket client
    $cache->{ $bucket_name } = $bucket;

    return $bucket;

}

1;
