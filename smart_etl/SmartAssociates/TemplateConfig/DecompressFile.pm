package SmartAssociates::TemplateConfig::DecompressFile;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use Archive::Tar::Wrapper;
use File::Basename;

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    # First the required ones ...
    my $filename    = $self->resolve_parameter( '#P_FILENAME#' )
        || $self->log->fatal( "Missing param [#P_FILENAME#]" );
    
    my $iterator_store_name = $self->resolve_parameter( '#P_ITERATOR#' )
        || $self->log->fatal( "Missing param [#P_ITERATOR#]" );
    
    # Check if file exists ...
    if ( ! -e $filename ) {
        $self->log->fatal( "File [$filename] doesn't exist" );
    }
    
    # Users can set the compression type manually - useful if file doesn't have an extension
    my $compression_type = $self->resolve_parameter( '#P_COMPRESSION_TYPE#' );
    
    if ( ! $compression_type ) {
        if ( $file =~ /.*\.([\w*])$/ ) {
            $compression_type = $1;
        } else {
            $self->log->fatal( "No #P_COMPRESSION_TYPE# passed, and failed to parse out of filename [$filename]" );
        }
    }
    
    my ( $tar_file_name , $source_dir , $suffix ) = fileparse( $filename );
    
    my $delete_original  = $self->resolve_parameter( '#P_DELETE_ORIGINAL#' );
    
    my $template_text    = $self->detokenize( $template_config->{TEMPLATE_TEXT} );
    
    my ( $all_objects , $object_count );
    
    eval {
        
        my $arch = Archive::Tar::Wrapper->new();
        $arch->read( $file );
        
        # Iterate over all entries in the archive
        $arch->list_reset(); # Reset Iterator
        
        while( my $entry = $arch->list_next() ) {
            my ( $tar_path , $phys_path ) = @$entry;
            # move back to source dir
            move( $phys_path , $source_dir );
            my $this_file = basename( $phys_path );
            my $this_path = $source_dir . "/" . $this_file );
            # add to iterator
            push @{$all_objects}
              , {
                    path    => $this_path
                };
            $object_count ++;
        }
        
        if ( $delete_original ) {
            unlink $filename
                or die( "Could not unlink $filename: $!" );
        }
        
    };
    
    my $iterator = SmartAssociates::Iterator->new(
        $self->globals
      , $iterator_store_name
      , $all_objects
    );
    
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
      , $object_count
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
