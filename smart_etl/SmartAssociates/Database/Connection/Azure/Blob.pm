package SmartAssociates::Database::Connection::Azure::Blob;

use strict;
use warnings;

# For Capturing STDERR, IPC etc
use IPC::Open3;
use IO::Select;

use File::Basename;
use File::Path qw | make_path |;
use File::Spec;
use JSON;

use Net::Azure::StorageClient;
use Net::Azure::StorageClient::Blob;

use XML::Simple;
use MIME::Base64;

use base 'SmartAssociates::Database::Connection::Base';

my $IDX_BLOB_CLIENT                           =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_ACCOUNT_NAME                          =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_ACCOUNT_KEY                           =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_ENDPOINT                              =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 3;

use constant FIRST_SUBCLASS_INDEX                                                                                 => 4;

use constant DB_TYPE                            => 'Azure::Blob';

sub connect_do {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    # Store these in properly named attributes so we're not translating from our regular auth hash key names
    
    $self->[ $IDX_ACCOUNT_NAME ]  = $auth_hash->{Username};
    $self->[ $IDX_ACCOUNT_KEY ]   = $auth_hash->{Password};
    
    $self->dbh( {} );
    
}

sub AZURE_BLOB_PUT {
    
    my ( $self , $template_config_class ) = @_;
    
    # Azure libraries and documentation are just ATROCIOUS. None of the Perl libraries are able to put blobs :/
    # However the az cli *is* able to, so we simply shell out to this. This is probably the safest long-term option
    # anyway, as Microsoft are more likely to fix their own CLI than their API documentation to allow library
    # developers to track their constantly changing BS
    
    my ( $container_name , $target_path , $source_file_path , $TEMPLATE_TEXT , $storage_path_prefix , $az_path );
    
    $container_name      = $template_config_class->resolve_parameter( '#P_CONTAINER_NAME#' ) || die( "Missing required param: [#P_CONTAINER_NAME#]" );
    $storage_path_prefix = $template_config_class->resolve_parameter( '#P_STORAGE_PATH_PREFIX#' ) || die( "Missing required param: [#P_STORAGE_PATH_PREFIX#]" );
    $target_path         = $template_config_class->resolve_parameter( '#P_TARGET_PATH#' ) || die( "Missing required param: [#P_TARGET_PATH#]" );
    $source_file_path    = $template_config_class->resolve_parameter( '#P_SOURCE_FILE_PATH#' ) || die( "Missing required param: [#P_SOURCE_FILE_PATH#]" );
    $az_path             = $template_config_class->resolve_parameter( '#P_AZ_PATH#' ) || die( "Missing required param: [#P_AZ_PATH#]" );
    
    $self->[ $IDX_BLOB_CLIENT] = Net::Azure::StorageClient::Blob->new(
        account_name       => $self->[ $IDX_ACCOUNT_NAME ]
      , primary_access_key => $self->[ $IDX_ACCOUNT_KEY ]
      , [ container_name   => $container_name ]
      , [ protocol         => 'https' ]
    );
    
    $TEMPLATE_TEXT       = $template_config_class->template_record()->{TEMPLATE_TEXT};
    $TEMPLATE_TEXT       = $template_config_class->detokenize( $TEMPLATE_TEXT );
    
    my @st = (
        $az_path
      , "storage"
      , "blob"
      , "upload"
      , "--account-name"   => $self->[ $IDX_ACCOUNT_NAME ]
      , "--container-name" => $container_name
      , "--name"           => $storage_path_prefix . $target_path
      , "--file"           => $source_file_path
      , "--account-key"    => $self->[ $IDX_ACCOUNT_KEY ]
      , "--overwrite"
    );

    # Probably won't need CMD_IN
    my $pid;

    # Note: inside this eval{} block, I'm calling die() for fatal errors instead of $self->log->fatal.
    # This will get caught at the end of the eval{} block, and the error we passed to die() will get returned
    # to the caller ( our parent class ), which will log the SQL executed, the fact that there was an error, and
    # *then* it will call $self->log->fatal.

    eval {

        no warnings 'uninitialized';

        $pid = open3(*CMD_IN, *CMD_OUT, *CMD_ERR, @st)
            || die( "Failure in launching az!\n" . $!);

        my $exit_status = 0;

        # We need to fetch/set the endpoint for later ( the COPY command )
        my $containers_response = $self->list_containers();
        $self->[ $IDX_ENDPOINT ] = $containers_response->{AccountName};
        my $query_parameters = $self->globals->Q_PARAMS;
        $query_parameters->{AZURE_BLOB_STORAGE_ENDPOINT} = $self->[ $IDX_ENDPOINT ];
        $self->globals->Q_PARAMS( $query_parameters );

        $self->log->info( "Launching Azure Blob put ..." );

        # NP TODO Expand on signal handling a bit, these are just stubs.. Most of our IPC is done by the .ready file mechanism at the moment, but there's no reason we can't expand on this
        $SIG{CHLD} = sub {
            if ( waitpid( $pid, 0 ) > 0) {
                $exit_status = $?;
            }
        };

        $SIG{TERM} = sub {
            die( "SIGTERM received ... exiting" );
        };

        # Allows the child to take input on STDIN.. Again, not required, just a stub
        print CMD_IN "Howdy...\n";
        close( CMD_IN );

        my $selector = IO::Select->new();
        $selector->add(*CMD_ERR, *CMD_OUT);

        my $errtxt = "";
        my $outtxt = "";

        while ( my @ready = $selector->can_read() ) {
            foreach my $fh ( @ready ) {
                my $t = "";
                if ( fileno( $fh ) == fileno( CMD_ERR ) ) {
                    $t = scalar <CMD_ERR>;
                    $errtxt .= $t if $t;
                } else {
                    $t = scalar <CMD_OUT>;
                    $outtxt.=$t if $t;
                    # if ( $t && $t =~ /^(\d*) rows copied/ ) {
                    #     $rows = $1;
                    # }
                }
                $selector->remove( $fh ) if eof( $fh );
            }
        }

        if ( $exit_status && $errtxt ) {
            die( "az exited with status [$exit_status], and error text:\n$errtxt\n\nshort error text:\n$outtxt" );
        }

        # Should be safe for Dan's logging stuff
        close( CMD_OUT );
        close( CMD_ERR );

    };

    my $error = $@;

    return {
        record_count  => 1
      , error         => $error
      , template_text => $TEMPLATE_TEXT
    };

}

sub AZURE_BLOB_PUT_broken_libs {

    my ( $self , $template_config_class ) = @_;
    
    my ( $blob_client , $container_name , $target_path , $source_file_path , $TEMPLATE_TEXT , $storage_path_prefix );
    
    my $response_hash;

    $container_name      = $template_config_class->resolve_parameter( '#P_CONTAINER_NAME#' ) || die( "Missing required param: [#P_CONTAINER_NAME#]" );
    $storage_path_prefix = $template_config_class->resolve_parameter( '#P_STORAGE_PATH_PREFIX#' ) || die( "Missing required param: [#P_STORAGE_PATH_PREFIX#]" );

    $self->[ $IDX_BLOB_CLIENT] = Net::Azure::StorageClient::Blob->new(
        account_name       => $self->[ $IDX_ACCOUNT_NAME ]
      , primary_access_key => $self->[ $IDX_ACCOUNT_KEY ]
      , [ container_name   => $container_name ]
      , [ protocol         => 'https' ]
    );

    eval {
        
        $blob_client = $self->[ $IDX_BLOB_CLIENT ];

        $target_path      = $template_config_class->resolve_parameter( '#P_TARGET_PATH#' ) || die( "Missing required param: [#P_TARGET_PATH#]" );
        $source_file_path = $template_config_class->resolve_parameter( '#P_SOURCE_FILE_PATH#' ) || die( "Missing required param: [#P_SOURCE_FILE_PATH#]" );
        
        if ( ! -e $source_file_path ) {
            die( "Source file [$source_file_path] doesn't exist!" );
        }
        
        $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
        $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
        
        $source_file_path =~ s/\/\//\//g; # remove double slashes - we might need to handle paths "properly"
        
        my $size = `du -h $source_file_path`;
        chomp( $size );
        
        $self->log->info( "Source file [$source_file_path] is [$size]" );

        my $containers_response = $self->list_containers();
        $self->[ $IDX_ENDPOINT ] = $containers_response->{AccountName};

#        $self->log->info( "Beginning Azure Blob PUT ..." );        
#        $response_hash = $self->put_blob( $source_file_path , $container_name , $target_path );

        $self->log->info( "Beginning Azure Blob upload ..." );
        
        # The library we're using ( Net::Azure::StorageClient::Blob ) doesn't support
        # splitting a file into multi-part uploads, so we do it ourselves ...
        
        my $size = 10_000_000; # approximate size of output files
        my $count = 1;
        open my $in_fh, '<', $source_file_path or die $!;
        binmode $in_fh;
        my @block_ids;
        
        while ( 1 ) {
        
            my $outfile = sprintf "%s.%05d", $source_file_path, $count++;
            open my $out_fh, '>', $outfile or die $!;
            binmode $out_fh;
            last unless read( $in_fh , my $buf , $size );
            print $out_fh $buf;
            close $out_fh;

            my $base64_outfile = encode_base64( $outfile );
            my $params = { options => "blockid=$base64_outfile" };
            my $resp = $blob_client->put_block( $outfile , $params );
            my $response = $self->handle_response( $resp );
            print( to_json( $response , { pretty => 1 } ) );
        
            push @block_ids , { Latest => $count }; # TODO: parse block ID from response and replace $count
            
            unlink( $outfile );
            
        }
        
        my $params = { BlockList => \@block_ids };
        my $res = $blob_client->put_block_list( $storage_path_prefix . $target_path , $params );
        my $response = $self->handle_response( $res );
        print( to_json( $response , { pretty => 1 } ) );
        
        close $in_fh;
        
        $self->log->info( "Azure Blob PUT has ended" );
        
        my $query_parameters = $self->globals->Q_PARAMS;
        $query_parameters->{AZURE_BLOB_STORAGE_ENDPOINT} = $self->[ $IDX_ENDPOINT ];
        $self->globals->Q_PARAMS( $query_parameters );
        
    };
    
    my $error_string = $@;
    
    # $self->log->info( "Status of S3 PUT request: [$put_status]" );
    
    # Fetch the storage account details, and set a Q_PARAM
    # ( better than trying to assemble based on account name etc - dynamic )
    # This allows subsequent steps ( eg an Azure Synapse COPY command ) to fetch the storage account URL
    # from a Q_PARAM
    
    return {
        record_count  => 1
      , error         => $error_string
      , template_text => $TEMPLATE_TEXT
    };
    
}

sub put_blob {
    
    my ( $self , $local_filename , $container_name , $path ) = @_;
    
    # The Put Blob operation creates a new block blob or page blob, or updates
    # the content of an existing block blob.
    #     http://msdn.microsoft.com/en-us/library/windowsazure/dd179451.aspx
    
    # This puts *data* to a file:
    # my $res = $self->[ $IDX_BLOB_CLIENT]->put_blob( $path, $data );
    
    # This uploads local file to a blob.
    my $params = { filename => $local_filename };
#    my $res = $self->[ $IDX_BLOB_CLIENT ]->put_blob( $container_name . "/" . $path, $params );
    my $res = $self->[ $IDX_BLOB_CLIENT ]->upload( $container_name   . "/" . $path , $local_filename );
    
    my $response_hash = $self->handle_response( $res );

    if ( $response_hash->{_msg} eq 'Created' ) {
        return $response_hash;
    } else {
        die( # we're in an eval block, above
            "put_blob() didn't get expected _msg of 'Created':\n"
          . to_json( $response_hash , { pretty => 1 } )
        );
    }
    
}

sub list_containers {
    
    my ( $self , $params ) = @_;
    
    # The List Containers operation returns a list of the containers under the
    # specified account.
    # http://msdn.microsoft.com/en-us/library/windowsazure/dd179352.aspx
    
    my $res = $self->[ $IDX_BLOB_CLIENT]->list_containers( $params );
    
    my $resp_hash = XMLin( $res->content() );

    $self->log->debug( "list_containers() returned:\n" . $res->content() );
    
    return $resp_hash;
    
}

sub handle_response {
    
    my ( $self , $http_response ) = @_;

    my $response_hash;

    my $content = $http_response->content();

    if ( defined $content && $content ne '' ) {
        $response_hash = XMLin( $content );
        if ( exists $response_hash->{Code} ) {
            $self->log->fatal( to_json( $response_hash , { pretty => 1 } ) );
        }
        return $response_hash;
    }

    return $http_response;
    
}

sub S3_ITERATOR {
    
    my ( $self, $template_config_class ) = @_;
    
    my ( $iterator , $TEMPLATE_TEXT );
    
    eval {
    
        my $iterator_store_name = $template_config_class->resolve_parameter( '#P_ITERATOR#' )
            || die( "Iterators must define the [#P_ITERATOR#] parameter" );
        
        my $bucket_name = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' )
            || die( "Missing parameter [#P_BUCKET#]" );
        
        my $prefix      = $template_config_class->resolve_parameter( '#P_PREFIX#' );
        
        my $bucket = $self->[ $IDX_BLOB_CLIENT ]->bucket( $bucket_name )
            || die( "Encountered error initializing bucket [$bucket_name]: " . $! );
        
        my $response = $bucket->list_all( { bucket => $bucket_name , prefix => $prefix } )
            or die( $self->[ $IDX_BLOB_CLIENT ]->err . ": " . $self->[ $IDX_BLOB_CLIENT ]->errstr );
        
        $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
        $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
        
        my $all_objects = [];
        
        foreach my $key ( @{ $response->{keys} } ) {
            my $key_name = $key->{key};
            my $key_size = $key->{size};
            $self->log->debug( "Bucket contains key [$key_name] of size [$key_size]" );
            my ( $s3_key_filename_part , $s3_key_dir_part , $suffix ) = fileparse( $key_name );
            push @{$all_objects}
              , {
                    KEY  => $key_name
                  , SIZE => $key->{size}
                  , PATH => $s3_key_dir_part
                  , FILE => $s3_key_filename_part
                };
        }
        
        $iterator = SmartAssociates::Iterator->new(
            $self->globals
          , $iterator_store_name
          , $all_objects
        );
        
        $self->globals->ITERATOR( $iterator_store_name, $iterator );
        
    };
    
    my $error = $@;
    
    return {
        record_count  => $iterator->count_items
      , error         => $error
      , template_text => $TEMPLATE_TEXT
    };
    
}

1;
