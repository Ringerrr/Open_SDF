package SmartAssociates::TemplateConfig::GoogleCloudStorage;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    my $operation       = $self->resolve_parameter( '#P_OPERATION#' )            || $self->log->fatal( "Mising param #P_OPERATION#" );
    my $template_text   = $self->detokenize( $template_config->{TEMPLATE_TEXT} );
    
    my @args;
    
    if ( $operation eq 'put' ) {
        
        my $local_file_path         = $self->resolve_parameter( '#P_LOCAL_FILE_PATH#' )     || $self->log->fatal( "Mising param #P_LOCAL_FILE_PATH#" );
        my $destination_bucket      = $self->resolve_parameter( '#P_DESTINATION_BUCKET#' )  || $self->log->fatal( "Mising param #P_DESTINATION_BUCKET#" );
        
        @args = (
            "gsutil"
          , "cp"
          , $local_file_path
          , $destination_bucket
        );
        
    } elsif ( $operation eq 'get' ) {
        
        my $google_cloud_storage_path   = $self->resolve_parameter( '#P_GOOGLE_CLOUD_STORAGE_PATH#' )  || $self->log->fatal( "Mising param #P_GOOGLE_CLOUD_STORAGE_PATH#" );
        my $local_destination_folder    = $self->resolve_parameter( '#P_LOCAL_DESTINATION_FOLDER#' )  || $self->log->fatal( "Mising param #P_LOCAL_DESTINATION_FOLDER#" );
        
        @args = (
            "gsutil"
          , "cp"
          , $google_cloud_storage_path
          , $local_destination_folder
        );
        
    } else {
        
        $self->log->fatal( "Unknown operation: [$operation]" );
        
    }
    
    eval {
        
        my $status = system( @args );
        
        if ( $status ) {
            die( "Received a non-zero status [$status] from gsutil" );
        }
        
    };
    
    my $error = $@;
    
    my $end_ts = $self->log->prettyTimestamp();
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , undef
      , undef
      , undef
      , $start_ts
      , $end_ts
      , ( $error ? 0 : 1 )
      , $error
      , $template_text
      , undef
      , $template_config->{NOTES}
    );
    
    
}

1;
