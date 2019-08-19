package SmartAssociates::TemplateConfig::SQL::Postgres_Remote_Copy;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::SQL';

use constant VERSION                            => '1.1';

sub handle_executed_sth {
    
    my ( $self, $sth ) = @_;
    
    my $file_path           = $self->resolve_parameter( '#P_FILE_PATH#' )               || $self->log->fatal( "Missing #P_FILE_PATH#" );
    
    $self->log->info( "Opening file: [" . $file_path . "]" );
    
    my $file;
    
    open $file, "<", $file_path
        || $self->log->fatal( "Couldn't open the pipe for reading!\n" . $! );
    
    my $counter;
    
    my $dbh = $self->target_database->dbh;
    
    while ( my $line = <$file> ) {
        $dbh->pg_putcopydata( $line );
        $counter ++;
        if ( $counter % 10000 == 0 ) {
            my $formatted_counter = $counter;
            $formatted_counter =~ s/(\d)(?=(\d{3})+(\D|$))/$1\,/g;
            $self->log->info( "$formatted_counter lines ..." );
        }
    }
    
    $dbh->pg_putcopyend
        || die( "Failed to complete Postgres COPY operation!\n" . $dbh->errstr );
    
    close $file
        or die( "Failed to close pipe!\n" . $! );
    
    $self->log->info( "Postgres Remote Copy has written [$counter] LINES ( not records, lines ) to the native pg client" );
    
    return $counter;
    
}

1;
