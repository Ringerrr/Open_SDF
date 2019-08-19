package SmartAssociates::TemplateConfig::SQL::JSON_Writer;

use strict;
use warnings;

use Text::CSV;

use base 'SmartAssociates::TemplateConfig::SQL';

use constant VERSION                            => '1.1';

sub handle_executed_sth {
    
    my ( $self, $sth ) = @_;
    
    my $file_path           = $self->resolve_parameter( '#P_FILE_PATH#' )               || $self->log->fatal( "Missing #P_FILE_PATH#" );
    my $page_size           = $self->resolve_parameter( '#P_PAGE_SIZE#' );
    
    my $json_encoder = JSON::XS->new->utf8;
    
    $self->log->info( "Opening file: [" . $file_path . "]" );
    
    open my $json_file, ">utf8", $file_path
        || $self->log->fatal( "Failed to open file for writing: [$file_path]\n" . $! );
    
    $self->log->info( "File open. Starting to write ..." );
    
    my $counter = 0;
    my $page_no = 0;
    
    $self->perf_stat_start( 'Fetch records from database' );
    
    while ( my $record = $sth->fetchrow_hashref ) {
        
        $self->perf_stat_stop( 'Fetch records from database' );
        $self->perf_stat_start( 'Write records to new-line delimited JSON' );
        
        print $json_file $json_encoder->encode( $record ) . "\n";
        $counter ++;
        
        $self->perf_stat_stop( 'Write records to new-line delimited JSON' );
        $self->perf_stat_start( 'Fetch records from database' );
        
        if ( $counter % 1000 == 0 ) {
            $self->log->info( "Written [" . $self->comma_separated( $counter ) . "] records so far" );
        }
        
    };
    
    $self->perf_stat_stop( 'Fetch records from database' );
    
    if ( $counter % 1000 != 0 ) {
        $self->log->info( "Written [" . $self->comma_separated( $counter ) . "] records so far" );
    }
    
    close $json_file
        or $self->log->fatal( "Failed to close file!\n" . $! );
    
    $self->log->info( "JSON Writer has written [" . $self->comma_separated( $counter ) . "] records" );
    
    return $counter;
    
}

1;
