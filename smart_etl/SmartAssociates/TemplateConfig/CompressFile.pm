package SmartAssociates::TemplateConfig::CompressFile;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.3';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    # First the required ones ...
    my $filename    = $self->resolve_parameter( '#P_FILENAME#' )
        || $self->log->fatal( "Missing param #P_FILENAME#" );
    
    my $compression_type = $self->resolve_parameter( '#P_COMPRESSION_TYPE#' );
    my $delete_original  = $self->resolve_parameter( '#P_DELETE_ORIGINAL#' );
    
    my $template_text    = $self->detokenize( $template_config->{TEMPLATE_TEXT} );
    
    eval {
        
        my $app_path;
        
        if ( $compression_type eq 'gzip' ) {
            
            # First check for pigz - a parallel compression utility
            $app_path = "/usr/bin/pigz";
            
            if ( -f $app_path ) {
                
                $self->log->info( "Parallel zipping ( via pigz ) file ..." );
                
                my @args = (
                    $app_path
                  , "-f"
                  , "-k"
                  , $filename
                );
                
                system( @args ) == 0
                    or die( "pigz compression failed: " . $? );
                
            } else {
                
                $app_path = "/bin/gzip";
                                
                if ( -f $app_path ) {
                    
                    $self->log->info( "Zipping ( via gzip ) file ..." );
                    
                    my @args = (
                        $app_path
                      , $filename
                    );
                    
                    system( @args ) == 0
                        or die( "gzip compression failed: " . $? );
                    
                } else {
                    
                    die( "Config requested {gzip} type, but neither pigz nor gzip binary installed" );
                    
                }
                
            }
            
        } elsif ( $compression_type eq 'bzip2' ) {
            
            $app_path = "/bin/bzip2";
            
            if ( -f $app_path ) {
                
                $self->log->info( "Zipping ( via bzip2 ) file ..." );
                
                my @args = (
                    $app_path
                  , $filename
                );
                
                system( @args ) == 0
                    or die( "bzip2 compression failed: " . $? );
                
            } else {
                
                die( "Config requested {bzip2} type, but bzip2 binary not installed" );
                
            } 
            
        } elsif ( $compression_type eq 'zip' ) {
            
            $app_path = "/usr/bin/zip";
            
            if ( -f $app_path ) {
                
                $self->log->info( "Zipping ( via zip ) file ..." );
                
                my @args = (
                    $app_path
                  , $filename . ".zip"
                  , $filename
                );
                
                system( @args ) == 0
                    or die( "zip compression failed: " . $? );
                
            } else {
                
                die( "Config requested {zip} type, but zip binary not installed" );
                
            } 
            
        } else {
            
            die( "Unknown compression type: {$compression_type}" );
            
        }
        
        if ( $delete_original ) {
            
            unlink $filename
                or die( "Could not unlink $filename: $!" );
            
        }
        
    };
    
    my $error = $@;
    
    my $end_ts = $self->log->prettyTimestamp();
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $self->detokenize( $template_config->{TARGET_DB_NAME} )
      , $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} )
      , $self->detokenize( $template_config->{TARGET_TABE_NAME} )
      , $start_ts
      , $end_ts
      , 0
      , $error
      , $template_text
      , undef
      , $template_config->{NOTES}
    );
    
    if ( $error ) {
        $self->log->fatal( $error );
    }
    
}

1;
