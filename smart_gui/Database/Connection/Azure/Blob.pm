package Database::Connection::Azure::Blob;

use parent 'Database::Connection';

use strict;
use warnings;

use JSON;

use Net::Azure::StorageClient;
use Net::Azure::StorageClient::Blob;

use XML::Simple;
use MIME::Base64;

use Glib qw | TRUE FALSE |;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'char2hexint';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'Azure';

sub connection_label_map {

    my $self = shift;

    return {
        Username        => "Account Name"
      , Password        => "Account Key"
      , Database        => ""
      , Host_IP         => ""
      , Port            => ""
      , Attribute_1     => "Shared Access Secret"
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };

}

sub get_blob {

    my ( $self , $path ) = @_;

    my $res = $self->{connection}>get_blob( $path );

    # Request with custom http headers and query.
    my $params = { headers => { 'x-ms-foo' => 'bar' }
                 , options => 'timeout=90' };

    $res = $self->{connection}->set_metadata( $path, $params );

}

sub create_container {

    my ( $self , $container_name ) = @_;

    my $res = $self->{connection}->create_container( $container_name );

}

sub list_containers {

    my ( $self , $params ) = @_;

    # The List Containers operation returns a list of the containers under the
    # specified account.
    # http://msdn.microsoft.com/en-us/library/windowsazure/dd179352.aspx

    my $res = $self->{connection}->list_containers( $params );

    my $response_hash = $self->handle_response( $res );

    my $type = ref $response_hash->{Containers}->{Container};

    # Return type morphs between HASH and ARRAY, depending on the number of items :/
    if ( ref $response_hash->{Containers}->{Container} eq "ARRAY" ) {
        return $response_hash->{Containers}->{Container};
    } else {
        my @ret;
        push @ret , $response_hash->{Containers}->{Container};
        return \@ret;
    }

}

sub put_blob {

    my ( $self , $local_filename , $path ) = @_;

    # The Put Blob operation creates a new block blob or page blob, or updates
    # the content of an existing block blob.
    #     http://msdn.microsoft.com/en-us/library/windowsazure/dd179451.aspx

    # This puts *data* to a file:
    # my $res = $self->{connection}->put_blob( $path, $data );

    # This uploads local file to a blob.
    #my $params = { filename => $local_filename };
    #my $res = $self->{connection}->put_blob( $path, $params );
    
    #my $response_hash = $self->handle_response( $res );
    
    #return $response_hash;
    
    # The library we're using ( Net::Azure::StorageClient::Blob ) doesn't support
    # splitting a file into multi-part uploads, so we do it ourselves ...
    
    my $size = 10_000_000; # approximate size of output files
    my $count = 1;
    open my $in_fh, '<', $local_filename or die $!;
    binmode $in_fh;
    my @block_ids;
    
    while ( 1 ) {
        
        my $outfile = sprintf "%s.%05d", $local_filename, $count++;
        open my $out_fh, '>', $outfile or die $!;
        binmode $out_fh;
        last unless read( $in_fh , my $buf , $size );
        print $out_fh $buf;
        close $out_fh;

        my $block_id = encode_base64( $outfile );
        my $params = { options => "blockid=$block_id" };
        my $resp = $self->{connection}->put_block( $local_filename , $params );
        my $response = $self->handle_response( $resp );
        print( to_json( $response , { pretty => 1 } ) );
        
        push @block_ids , { Latest => $block_id };
        
        unlink( $outfile );
        
    }
    
    my $params = { BlockList => \@block_ids };
    my $resp = $self->{connection}->put_block_list( $path, $params );
    my $response = $self->handle_response( $resp );
    print( to_json( $response , { pretty => 1 } ) );
    
    close $in_fh;
    
}

sub handle_response {

    my ( $self , $response ) = @_;

    my $response_hash = XMLin( $response->content() );

    if ( exists $response_hash->{Code} ) {
        $self->dialog(
            {
                title   => "Error in API call"
              , type    => "error"
              , text    => to_json( $response_hash , { pretty => 1 } )
            }
        );
    }

    return $response_hash;

}
sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    eval {

        require Azure::Storage::Blob::Client;

        $self->{connection} = Net::Azure::StorageClient::Blob->new(
            account_name       => $auth_hash->{Username}
          , primary_access_key => $auth_hash->{Password}
          , [ container_name   => '' ]
          , [ protocol         => 'https' ]
        );
        
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title   => "Failed to connect to Azure Blog storage"
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
    
    my $response = $self->list_containers();
    
    my @return;

    foreach my $container_obj ( @{ $response } ) {
        push @return, $container_obj->{Name};
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
