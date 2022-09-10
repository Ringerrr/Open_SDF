package Database::Connection::Google;

use parent 'Database::Connection';

use strict;
use warnings;

use Glib qw | TRUE FALSE |;

use Exporter qw ' import ';

use constant DB_TYPE    => 'Google';

sub connection_label_map {

    my $self = shift;

    return {
        Username        => "Client ID"
      , Password        => "Refresh Token"
      , Database        => ""
      , Host_IP         => "Client Secret"
      , Port            => ""
      , Attribute_1     => "Project ID"
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

        require Net::Google::Storage;
        require Net::Google::Storage::Agent;

        # Attempt connection to GCP and retrieve buckets
        $self->{GoogleStorage} = Net::Google::Storage->new(
            projectId        => $auth_hash->{Attribute_1}
          , refresh_token    => $auth_hash->{Password}
          , client_id        => $auth_hash->{Username}
          , client_secret    => $auth_hash->{Host}
        );

        my @response = $self->fetch_database_list();
        foreach my $bucket ( @response ) {
            print( $bucket . "\n" );
        }

    };

    my $err = $@;

    if ( $err ) {
        $self->dialog(
            {
                title   => "Failed to list GCS buckets"
              , type    => "error"
              , text    => $err
            }
        );
        return undef;
    }


    return 1;

}

sub fetch_database_list {
    
    my $self = shift;
    
    my $response = $self->{GoogleStorage}->list_buckets();
    
    my @return;
    
    foreach my $bucket_obj ( @{$response} ) {
        push @return, $bucket_obj->id();
    }
    
    return sort( @return );
    
}

sub can_execute_ddl {

    my $self = shift;

    return FALSE;

}

sub has_odbc_driver {

    my $self = shift;

    return FALSE;

}

1;
