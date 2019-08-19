package SmartAssociates::ProcessingGroup::Compress;

use strict;
use warnings;

use base 'SmartAssociates::ProcessingGroup::Ingestion';

sub prepare {
    
    my $self = shift;
    
    $self->setupPaths();
    
}

sub execute {
    
    my $self = shift;
    
    foreach my $dir_part ( 'nznotforload', 'nzfail', 'nzprocessed' ) {
        
        my $full_dir = $self->source_system_dir . "/" . $dir_part;
        
        opendir( DIR, $full_dir );
        my @fileList = readdir( DIR );
        closedir( DIR );
        
        # Loop through all files in the directory
        foreach my $file ( @fileList ) {
            
            next if ( $file =~ m/^\./ );        # Don't process '.' files
            next if ( $file =~ m/\.gz/ );       # Don't process gzipped files
            
            my $file_path = $full_dir . "/" . $file;
            $self->log->info( "gzipping $file_path" );
            my $output = `gzip $file_path &`;
            
        }
        
    }
    
}

1;
