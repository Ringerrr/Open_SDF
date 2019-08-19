package SmartAssociates::TemplateConfig::RegexToCSV;

use strict;
use warnings;

use Text::CSV;
use File::Copy;

use JSON;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.1';

sub execute {
    
    my $self = shift;
    
    # This class is to parse the contents of a file using a regular expression,
    # and write the resulting columns out to a CSV. A .good and .bad file are written.
    # Records we can't parse are in the .bad file
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    # First the required ones ...
    my $filename                    = $self->resolve_parameter( '#P_FILENAME#' )                || $self->log->fatal( "Missing param #P_FILENAME#" );
    
    my $input_encoding              = ":encoding(" . $self->resolve_parameter( '#P_INPUT_ENCODING#' ) . ")";
    my $output_encoding             = ":encoding(" . $self->resolve_parameter( '#P_OUTPUT_ENCODING#' ) . ")";
    my $escape_character            = $self->resolve_parameter( '#P_ESCAPE_CHARACTER#' );
    
    my $parser_regex                = $self->resolve_parameter( '#P_REGEX#' )                   || $self->log->fatal( "Missing param #P_REGEX#" );
    my $compiled_regex              = qr/$parser_regex/;
    
    my $error_state_regex           = $self->resolve_parameter( '#P_ERROR_STATE_REGEX#' );
    my $compiled_error_state_regex;
    
    my $max_parse_errors            = $self->resolve_parameter( '#P_MAX_PARSE_ERRORS#' );
    
    if ( $error_state_regex ) {
        $compiled_error_state_regex = qr/$error_state_regex/;
    }
    
    my $good_output_filename        = $filename . ".good";
    my $bad_output_filename         = $filename . ".bad";
    
    my ( $input, $good_output, $bad_output, $log );
    
    {
            
        no warnings 'uninitialized';
        
        if ( ! -e $filename ) {
            die( "Input file [$filename] doesn't exist!" );
        }
        
        my $open_string = "<" . $input_encoding;
        
        open $input, $open_string, $filename
            || die( "Failed to open input file [$filename] for reading:\n" . $! );
        
        $open_string = ">" . $output_encoding;
        
        open $good_output, $open_string, $good_output_filename
            || die( "Failed to open output file [$good_output_filename] for writing:\n" . $! );
        
        #$self->globals->JOB->add_temporary_file( $good_output_filename );
        
        open $bad_output, $open_string, $bad_output_filename
            || die( "Failed to open output file [$bad_output_filename] for writing:\n" . $! );
        
    }
    
    my ( $row_counter, $execution_log_text );
    
    $execution_log_text     = $self->resolve_parameter( $template_config->{TEMPLATE_TEXT} );
    
    my $bad_records         = 0;
    my $error_state_records = 0;
    
    eval {
        
        my $csv_writer = Text::CSV->new(
        {
            quote_char              => '"'
          , binary                  => 0
          , eol                     => "\n"
          , sep_char                => ","
          , escape_char             => $escape_character
          , quote_space             => 1
          , blank_is_undef          => 1
          , always_quote            => 1
        } );
        
        $self->perf_stat_start( 'Inside regex engine' );
        while( <$input> ) {
            $self->perf_stat_stop( 'Inside regex engine' );
            
            $row_counter ++;
            
            my $this_line = $_;
            
            my @matches = $this_line =~ $compiled_regex;
            
            if ( @matches ) {
                
                $self->perf_stat_start( 'Encoding CSV - Text::CSV_XS' );
                $csv_writer->print(
                    $good_output
                  , \@matches
                );
                $self->perf_stat_stop( 'Encoding CSV - Text::CSV_XS' );
                
            } else {
                
                # Attempt to parse with the error state regex if we have one
                if ( $error_state_regex ) {
                    if ( $this_line =~ $compiled_error_state_regex ) {
                        $error_state_records ++;
                        next; # next line of input file
                    }
                }
                
                print $bad_output $this_line;
                $bad_records ++;
                
                if ( $bad_records > $max_parse_errors ) {
                    die( "Exceeded maximum parse errors" );
                }
                
            }
            
            if ( $row_counter % 10000 == 0 ) {
                $self->log->info( "[" . $self->comma_separated( $row_counter ) . "] lines" );
            }
            
            $self->perf_stat_start( 'Inside regex engine' );
        }
        $self->perf_stat_stop( 'Inside regex engine' );
        
    };
    
    close $good_output
        || die( "Failed to close output file:\n" . $! );
    
    close $bad_output
        || die( "Failed to close output file:\n" . $! );
    
    close $input
        || die( "Failed to close input file:\n" . $! );
        
    my $error = $@;
    my $end_ts = $self->log->prettyTimestamp();
    
    if ( $bad_records ) {
        $execution_log_text .= "Encountered [$bad_records] records that we couldn't parse";
    } else {
        $execution_log_text .= "Parsed all records successfully";
    }
    
    if ( $error_state_records ) {
        $execution_log_text .= "\n\nAlso encountered [$error_state_records] records that were matched by our error state regex ( not counted in bad record count )";
    }
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , undef
      , undef
      , undef
      , $start_ts
      , $end_ts
      , $row_counter
      , $error
      , $execution_log_text
      , to_json( $self->perf_stats, { pretty => 1 } )
      , $template_config->{NOTES}
    );
    
    if ( $error ) {
        $self->log->fatal( $error );
    }
    
}

1;
