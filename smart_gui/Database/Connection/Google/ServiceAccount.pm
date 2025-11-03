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
      , Host_IP         => "Service Account JSON File Path"
      , Port            => ""
      , Attribute_1     => "Bucket"
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };

}

sub connection_browse_title {

    my $self = shift;

    return "Select a Google Service Account JSON File";

}

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    eval {

        # Validate required fields
        unless ( $auth_hash->{Host} ) {
            die "Service Account JSON File Path (Host) is required";
        }

        unless ( -e $auth_hash->{Host} ) {
            die "Service Account JSON file does not exist: " . $auth_hash->{Host};
        }

        # Read and parse the JSON file
        use JSON qw(decode_json);
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

        # Test connection if bucket name is provided
        if ( $auth_hash->{Attribute_1} ) {
            my $bucket = $self->_get_bucket_client( $auth_hash->{Attribute_1} );

            # Attempt to list files to verify connectivity and permissions
            my $result = $bucket->list_files();

            # If we get here without exception, connection is successful
            my $file_count = 0;
            if ( $result->{items} ) {
                $file_count = scalar @{ $result->{items} };
            }

            print "Successfully connected to bucket '" . $auth_hash->{Attribute_1} . "' ($file_count files)\n";
        }

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
