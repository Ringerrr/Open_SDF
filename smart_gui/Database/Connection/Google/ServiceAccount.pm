package Database::Connection::Google::ServiceAccount;

use parent 'Database::Connection::Google';

use strict;
use warnings;

use Glib qw | TRUE FALSE |;
use JSON qw(decode_json);

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
      , Attribute_2     => "Service Account JSON File Path"
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
