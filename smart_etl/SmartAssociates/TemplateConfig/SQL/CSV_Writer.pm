package SmartAssociates::TemplateConfig::SQL::CSV_Writer;

use strict;
use warnings;

use Text::CSV;

use base 'SmartAssociates::TemplateConfig::SQL';

use constant VERSION                            => '1.6';

sub handle_executed_sth {
    
    my ( $self, $sth ) = @_;
    
    my $file_path           = $self->resolve_parameter( '#P_FILE_PATH#' )               || $self->log->fatal( "Missing #P_FILE_PATH#" );
    my $column_headers      = $self->resolve_parameter( '#P_COLUMN_HEADERS#' );
    my $column_separator    = $self->resolve_parameter( '#P_COLUMN_SEPARATOR#' );
    my $page_size           = $self->resolve_parameter( '#P_PAGE_SIZE#' );
    my $quote_character     = $self->resolve_parameter( '#P_QUOTE_CHARACTER#' );
    my $escape_character    = $self->resolve_parameter( '#P_ESCAPE_CHAR#' );
    my $encoding            = $self->resolve_parameter( '#P_ENCODING#' );
    my $always_quote        = $self->resolve_parameter( '#P_ZZ_ALWAYS_QUOTE#' );
    my $use_text_csv_xs     = $self->resolve_parameter( '#P_USE_TEXT_CSV_XS#' );
#    my $binary_mode         = $self->resolve_parameter( '#P_BINARY_MODE#' );
    my $null_value          = $self->resolve_parameter( '#P_NULL_VALUE#' );

    my $csv_writer;
    
    if ( $use_text_csv_xs ) {
        $csv_writer = Text::CSV->new(
            {
                quote_char     => $quote_character
#              , binary         => $binary_mode
              , eol            => "\n"
              , sep_char       => $column_separator
              , escape_char    => $escape_character
              , quote_space    => 1
              , blank_is_undef => 0
              , always_quote   => $always_quote
              , undef_str      => $null_value
            }
        );
    }
    
    $self->log->info( "Opening file: [" . $file_path . "]" );
    
    my $writing_directive = ">";
    
    if ( $encoding ) {
        $writing_directive .= ":encoding($encoding)";
        $self->log->info( "Opening file for writing with encoding directive: $writing_directive" );
    } else {
        $self->log->info( "Opening file for writing with NO encoding directive" );
    }
    
    open my $csv_file, $writing_directive, $file_path
        || $self->log->fatal( "Failed to open file for writing: [$file_path]\n" . $! );
    
    $self->log->info( "File open. Starting to write ..." );
    
    if ( $column_headers ) {
        my $fields;
        if ( $sth->can( 'column_names' ) ) { # Salesforce
            $fields = $sth->column_names;
        } else {
            $fields = $sth->{NAME_uc};       # everything else
        }
        print $csv_file join( $column_separator, @{$fields} ) . "\n";
    }
    
    my $counter = 0;
    my $page_no = 0;
    
    # Optimisation: put the loop inside the if(), and not the other way around
    
    if ( $use_text_csv_xs ) {
        
        $self->perf_stat_start( 'Fetch pages from database' );

        while ( my $page = $sth->fetchall_arrayref( undef, $page_size ) ) {

            $self->perf_stat_stop( 'Fetch pages from database' );
            $self->perf_stat_start( 'Write records to CSV via Text::CSV_XS' );
            $page_no ++;
            $self->log->info( "Fetched page [" . $self->comma_separated( $page_no ) . "]" );

            foreach my $record ( @{$page} ) {
                $csv_writer->print( $csv_file, $record );
                $counter ++;
            }

            $self->perf_stat_stop( 'Write records to CSV via Text::CSV_XS' );
            $self->perf_stat_start( 'Fetch pages from database' );

            $self->log->info( "Written [" . $self->comma_separated( $counter ) . "] records so far" );

        };
        
        $self->perf_stat_stop( 'Fetch pages from database' );
        
    } else {
        
        $self->perf_stat_start( 'Fetch pages from database' );
        
        while ( my $page = $sth->fetchall_arrayref( undef, $page_size ) ) {
            
            no warnings 'uninitialized';
            
            $self->perf_stat_stop( 'Fetch pages from database' );
            $self->perf_stat_start( 'Write records to CSV via Pure Perl' );
            $page_no ++;
            $self->log->info( "Fetched page [" . $self->comma_separated( $page_no ) . "]" );
            
            foreach my $record ( @{$page} ) {
                print $csv_file join( $column_separator, @{$record} ) . "\n";
                $counter ++;
            }
            
            $self->perf_stat_stop( 'Write records to CSV via Pure Perl' );
            $self->perf_stat_start( 'Fetch pages from database' );
            
            $self->log->info( "Written [" . $self->comma_separated( $counter ) . "] records so far" );
        };
        
        $self->perf_stat_stop( 'Fetch pages from database' );
        
    }
    
    close $csv_file
        or $self->log->fatal( "Failed to close file!\n" . $! );
    
    $self->log->info( "CSV Writer has written [" . $self->comma_separated( $counter ) . "] records" );
    
    return $counter;
    
}

1;
