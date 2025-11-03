package SmartAssociates::Database::Connection::Google::ServiceAccount;

use strict;
use warnings;

use JSON qw(decode_json);
use Google::Cloud::Storage::Bucket;

use base 'SmartAssociates::Database::Connection::Google';

use constant DB_TYPE => 'GoogleServiceAccount';

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    # Validate required fields
    unless ( $auth_hash->{Host} ) {
        die "Service Account JSON File Path (Host) is required";
    }

    unless ( -e $auth_hash->{Host} ) {
        die "Service Account JSON file does not exist: " . $auth_hash->{Host};
    }

    # Read and parse the JSON file
    open my $fh, '<', $auth_hash->{Host} or die "Cannot open " . $auth_hash->{Host} . ": $!";
    my $json_text = do { local $/; <$fh> };
    close $fh;

    my $service_account_data = decode_json($json_text);

    # Validate required fields in JSON
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
