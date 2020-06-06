package SmartAssociates::Database::Connection::AWS;

use strict;
use warnings;

use File::Basename;
use File::Path qw | make_path |;
use File::Spec;
use JSON;
use Net::Amazon::S3;
use Text::CSV;

use base 'SmartAssociates::Database::Connection::Base';

my $IDX_S3_CLIENT                             =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_ACCESS_KEY                            =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_CLIENT_SECRET                         =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_BUCKET_NAME                           =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 3;

use constant FIRST_SUBCLASS_INDEX                                                                                 => 4;

use constant DB_TYPE                            => 'AWS';

sub connect_do {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    # Store these in properly named attributes so we're not translating from our regular auth hash key names
    
    $self->[ $IDX_ACCESS_KEY ]    = $auth_hash->{Username};
    $self->[ $IDX_CLIENT_SECRET ] = $auth_hash->{Password};
    $self->[ $IDX_BUCKET_NAME ]   = $auth_hash->{Database};
    
    my $s3 = Net::Amazon::S3->new(
        {   aws_access_key_id     => $self->[ $IDX_ACCESS_KEY ],
            aws_secret_access_key => $self->[ $IDX_CLIENT_SECRET ],
            retry                 => 1
        }
    );
    
    my $response = $s3->buckets
        or $self->log->fatal(  $s3->err . ": " . $s3->errstr );
    
    $self->[ $IDX_S3_CLIENT ] = $s3;
    $self->dbh( {} );

}

sub S3_GET {

    my ( $self , $template_config_class ) = @_;
    
    my $s3                = $self->[ $IDX_S3_CLIENT ];
    my $bucket_name       = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' ) || $self->log->fatal( "Missing param [#P_BUCKET_NAME#]" );
    my $key               = $template_config_class->resolve_parameter( '#P_SOURCE_KEY#' )  || $self->log->fatal( "Missing param [#P_SOURCE_KEY#]" );
    my $target_path       = $template_config_class->resolve_parameter( '#P_TARGET_PATH#' );
    my $flatten_directory = $template_config_class->resolve_parameter( '#P_FLATTEN_DIRECTORY#' );
    
    my $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
    $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
    
    # If our target path has a filename, then we use that. Otherwise if the target path is a directory name,
    # then we parse the filename out of the source key and use that.
    
    my ( $target_file_part , $target_dir_part , $suffix ) = fileparse( $target_path );
    my ( $s3_key_filename_part , $s3_key_dir_part , $suffix ) = fileparse( $key );
    
    if ( ! $flatten_directory ) {
        $target_path = File::Spec->catfile( $target_path , $s3_key_dir_part );
    }

    my ( $create_errors , $error_string );
    my @created = make_path( $target_path , { error => \$create_errors } );

    if ( ! defined $target_file_part or $target_file_part eq '' ) {
        $target_path = File::Spec->catfile( $target_path , $s3_key_filename_part );
    }

    my $response;

    if ( @{$create_errors} ) {
        
        $error_string = join( "\n" , $create_errors );
        
    } else {
        
        my $bucket = $s3->bucket( $bucket_name ) || $self->log->fatal( "Failed to select S3 bucket [$bucket_name]: " . $! );
        
        $response = $bucket->get_key_filename( $key , 'GET' , $target_path )
            || ( $error_string = $s3->err . ": " . $s3->errstr );
        
    }
    
    return {
        record_count  => 1
      , error         => $error_string
      , template_text => $TEMPLATE_TEXT
    };
    
}

sub S3_PUT {

    my ( $self , $template_config_class ) = @_;
    
    my ( $s3 , $bucket , $bucket_name , $target_key , $source_file_path , $content_type , $TEMPLATE_TEXT );
    
    eval {
        
        $s3 = $self->[ $IDX_S3_CLIENT ];
        
        $bucket_name      = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' ) || die( "Missing required param: [#P_BUCKET_NAME#]" );
        $target_key       = $template_config_class->resolve_parameter( '#P_TARGET_KEY#' ) || die( "Missing required param: [#P_TARGET_KEY#]" );
        $source_file_path = $template_config_class->resolve_parameter( '#P_SOURCE_FILE_PATH#' ) || die( "Missing required param: [#P_SOURCE_FILE_PATH#]" );
        $content_type     = $template_config_class->resolve_parameter( '#P_CONTENT_TYPE#' );
        
        $bucket = $s3->bucket( $bucket_name );
        
        $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
        $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );
        
        $source_file_path =~ s/\/\//\//g; # remove double slashes - we might need to handle paths "properly"
        
        $bucket->add_key_filename(
            $target_key,
            $source_file_path,
            {
               content_type => $content_type
            }
        ) or die( $s3->err . ": " . $s3->errstr );
        
    };
    
    my $error_string = $@;
    
    return {
        record_count  => 1
      , error         => $error_string
      , template_text => $TEMPLATE_TEXT
    };
    
}

sub S3_DELETE {

    my ( $self , $template_config_class ) = @_;

    my ( $s3 , $bucket , $bucket_name , $target_key , $TEMPLATE_TEXT );

    eval {

        $s3 = $self->[ $IDX_S3_CLIENT ];

        $bucket_name      = $template_config_class->resolve_parameter( '#P_BUCKET_NAME#' ) || die( "Missing required param: [#P_BUCKET_NAME]" );
        $target_key       = $template_config_class->resolve_parameter( '#P_TARGET_KEY#' ) || die( "Missing required param: [#P_TARGET_KEY#]" );

        $bucket = $s3->bucket( $bucket_name );

        $TEMPLATE_TEXT = $template_config_class->template_record()->{TEMPLATE_TEXT};
        $TEMPLATE_TEXT = $template_config_class->detokenize( $TEMPLATE_TEXT );

        $bucket->delete_key(
            $target_key
        ) or die( $s3->err . ": " . $s3->errstr );

    };

    my $error_string = $@;

    return {
        record_count  => 1
      , error         => $error_string
      , template_text => $TEMPLATE_TEXT
    };

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
        
        my $bucket = $self->[ $IDX_S3_CLIENT ]->bucket( $bucket_name )
            || die( "Encountered error initializing bucket [$bucket_name]: " . $! );
        
        my $response = $bucket->list_all( { bucket => $bucket_name , prefix => $prefix } )
            or die( $self->[ $IDX_S3_CLIENT ]->err . ": " . $self->[ $IDX_S3_CLIENT ]->errstr );
        
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

sub s3_client                     { return $_[0]->accessor( $IDX_S3_CLIENT,                     $_[1] ); }

1;
