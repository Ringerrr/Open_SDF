package SmartAssociates::Database::Connection::Google;

use strict;
use warnings;

use File::Basename;
use File::Path qw | make_path |;
use File::Spec;
use JSON;
use Net::Google::Storage;
use Net::Google::Storage::Agent;
use Text::CSV;

use base 'SmartAssociates::Database::Connection::Base';

my $IDX_GCP_CLIENT                            =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 0;

use constant FIRST_SUBCLASS_INDEX                                                                                 => 1;

use constant DB_TYPE                          => 'Google';

sub connect_do {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    my $gcp_storage = Net::Google::Storage->new(
        projectId        => $auth_hash->{Attribute_1}
      , refresh_token    => $auth_hash->{Password}
      , client_id        => $auth_hash->{Username}
      , client_secret    => $auth_hash->{Host}
    );
    
    $self->[ $IDX_GCP_CLIENT ] = $gcp_storage;
    $self->dbh( {} );

}

sub GOOGLE_CLOUD_STORAGE_GET {
    
    my ( $self , $template_config_class ) = @_;
    
    my $gcp_storage       = $self->[ $IDX_GCP_CLIENT ];
    my $bucket_name       = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' ) || $self->log->fatal( "Missing param [#P_BUCKET_NAME#]" );
    my $key               = $template_config_class->resolve_parameter( '#P_SOURCE_KEY#' )  || $self->log->fatal( "Missing param [#P_SOURCE_KEY#]" );
    my $target_path       = $template_config_class->resolve_parameter( '#P_TARGET_PATH#' );
    my $flatten_directory = $template_config_class->resolve_parameter( '#P_FLATTEN_DIRECTORY#' );
    
    my $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
    $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
    
    # If our target path has a filename, then we use that. Otherwise if the target path is a directory name,
    # then we parse the filename out of the source key and use that.
    
    my ( $target_file_part , $target_dir_part , $suffix , $s3_key_filename_part , $s3_key_dir_part );
    
    ( $target_file_part , $target_dir_part , $suffix ) = fileparse( $target_path );
    ( $s3_key_filename_part , $s3_key_dir_part , $suffix ) = fileparse( $key );
    
    if ( ! $flatten_directory ) {
        $target_path = File::Spec->catfile( $target_path , $s3_key_dir_part );
    }
    
    my ( $create_errors , $error_string );
    my @created = make_path( $target_path , { error => \$create_errors } );
    
    if ( ! defined $target_file_part or $target_file_part eq '' ) {
        $target_path = File::Spec->catfile( $target_path , $s3_key_filename_part );
    }
    
    my ( $response , $record_count );
    
    if ( @{$create_errors} ) {
        $error_string = join( "\n" , $create_errors );
        $record_count = 0;
    } else {
        $record_count = $gcp_storage->download_object(
            bucket   => $bucket_name
          , object   => $key
          , filename => $target_path
        ) or 0;
    }
    
    return {
        record_count  => $record_count
      , error         => $error_string
      , template_text => $TEMPLATE_TEXT
    };
    
}

sub GOOGLE_CLOUD_STORAGE_PUT {

    my ( $self , $template_config_class ) = @_;
    
    my ( $gcp_storage , $bucket_name , $target_key , $source_file_path , $content_type , $TEMPLATE_TEXT );
    
    eval {
        
        $gcp_storage = $self->[ $IDX_GCP_CLIENT ];
        
        $bucket_name      = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' ) || die( "Missing required param: [#P_BUCKET_NAME#]" );
        $target_key       = $template_config_class->resolve_parameter( '#P_TARGET_KEY#' ) || die( "Missing required param: [#P_TARGET_KEY#]" );
        $source_file_path = $template_config_class->resolve_parameter( '#P_SOURCE_FILE_PATH#' ) || die( "Missing required param: [#P_SOURCE_FILE_PATH#]" );
#        $content_type     = $template_config_class->resolve_parameter( '#P_CONTENT_TYPE#' );
        
        if ( ! -e $source_file_path ) {
            die( "Source file [$source_file_path] doesn't exist!" );
        }
        
        $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
        $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
        
        $source_file_path =~ s/\/\//\//g; # remove double slashes - we might need to handle paths "properly"
        
        my $size = `du -h $source_file_path`;
        chomp( $size );
        
        $self->log->info( "Source file [$source_file_path] is [$size]" );
        
        $self->log->info( "Beginning GCP storage PUT ..." );
        
        my $object = $gcp_storage->insert_object(
            bucket   => $bucket_name
          , object   => { name => $target_key }
          , filename => $source_file_path
        );
        
        if ( ! defined $object ) {
            die( "Call to insert_object() didn't return an object ..." );
        }
        
        $self->log->info( "GCP storage PUT has ended" );
        
    };
    
    my $error_string = $@;
    
    return {
        record_count  => ( $error_string ? 0 : 1 )
      , error         => $error_string
      , template_text => $TEMPLATE_TEXT
    };
    
}

sub GOOGLE_CLOUD_STORAGE_DELETE {

    my ( $self , $template_config_class ) = @_;
    
    my ( $gcp_storage , $bucket_name , $target_key , $TEMPLATE_TEXT );
    
    eval {
        
        $gcp_storage = $self->[ $IDX_GCP_CLIENT ];
        
        $bucket_name      = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' ) || die( "Missing required param: [#P_BUCKET_NAME]" );
        $target_key       = $template_config_class->resolve_parameter( '#P_TARGET_KEY#' ) || die( "Missing required param: [#P_TARGET_KEY#]" );
        
        $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
        $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
        
        $gcp_storage->delete(
            bucket => $bucket_name
          , object => $target_key
        );

    };
    
    my $error_string = $@;
    
    return {
        record_count  => ( $error_string ? 0 : 1 )
      , error         => $error_string
      , template_text => $TEMPLATE_TEXT
    };
    
}

sub GOOGLE_CLOUD_STORAGE_ITERATOR {
    
    my ( $self, $template_config_class ) = @_;
    
    my ( $iterator , $TEMPLATE_TEXT , $gcp_storage );
    
    eval {
    
        my $iterator_store_name = $template_config_class->resolve_parameter( '#P_ITERATOR#' )
            || die( "Iterators must define the [#P_ITERATOR#] parameter" );
        
        my $bucket_name = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' )
            || die( "Missing parameter [#P_BUCKET#]" );
        
        # my $prefix      = $template_config_class->resolve_parameter( '#P_PREFIX#' );
        
        my $gcp_storage = $self->[ $IDX_GCP_CLIENT ];
        
        my $response = $gcp_storage->list_objects( $bucket_name );
        
        $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
        $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
        
        my $all_objects = [];
        
        foreach my $object ( @{ $response } ) {
            my $key_name = $object->name();
            my $md5_hash = $object->md5Hash();
            $self->log->debug( "Bucket contains key [$key_name] with hash [$md5_hash]" );
            my ( $filename_part , $dir_part , $suffix ) = fileparse( $key_name );
            push @{$all_objects}
              , {
                    KEY  => $key_name
                  , MD5  => $md5_hash
                  , PATH => $dir_part
                  , FILE => $filename_part
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

sub gcp_storage_client                     { return $_[0]->accessor( $IDX_GCP_CLIENT,                     $_[1] ); }

1;
