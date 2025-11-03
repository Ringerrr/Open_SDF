package SmartAssociates::Database::Connection::Google::ServiceAccount;

use strict;
use warnings;

use JSON qw(decode_json);
use lib '/Users/dankasak/src/perl_gcs';
use Google::Cloud::Storage::Bucket;

use base 'SmartAssociates::Database::Connection::Google';

use constant DB_TYPE => 'GoogleServiceAccount';

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    # Attribute_2 contains the path to the service account JSON file
    my $json_file = $auth_hash->{Attribute_2};

    unless ( $json_file ) {
        die "Service Account JSON file path (Attribute_2) is required";
    }

    unless ( -e $json_file ) {
        die "Service Account JSON file does not exist: $json_file";
    }

    # Read and parse the JSON file
    open my $fh, '<', $json_file or die "Cannot open $json_file: $!";
    my $json_text = do { local $/; <$fh> };
    close $fh;

    my $service_account_data = decode_json($json_text);

    # Validate required fields
    unless ( $service_account_data->{client_email} ) {
        die "Service account JSON missing 'client_email' field";
    }

    unless ( $service_account_data->{private_key} ) {
        die "Service account JSON missing 'private_key' field";
    }

    # Store auth credentials for later bucket access
    $self->auth_config({
        client_email => $service_account_data->{client_email},
        private_key  => $service_account_data->{private_key}
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
        client_email => $auth_config->{client_email},
        private_key  => $auth_config->{private_key},
        bucket_name  => $bucket_name
    });

    # Cache the bucket client
    $cache->{ $bucket_name } = $bucket;

    return $bucket;

}

1;
