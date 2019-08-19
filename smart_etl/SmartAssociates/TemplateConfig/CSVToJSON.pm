package SmartAssociates::TemplateConfig::CSVToJSON;

use strict;
use warnings;

use JSON;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.1';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    # First the required ones ...
    my $input_filename              = $self->resolve_parameter( '#P_INPUT_FILENAME#' )      || $self->log->fatal( "Missing param #P_INPUT_FILENAME#" );
    my $output_filename             = $self->resolve_parameter( '#P_OUTPUT_FILENAME#' )     || $self->log->fatal( "Missing param #P_OUTPUT_FILENAME#" );
    
    my $input_eol_character         = $self->resolve_parameter( '#P_INPUT_EOL_CHARACTER#' );
    
    # Swap string literal \n
    if ( $input_eol_character eq '\n' ) {
        $input_eol_character = "\n";
    }
    
    my $allow_loose_quotes          = $self->resolve_parameter( '#P_ALLOW_LOOSE_QUOTES#' );
    my $allow_whitespace            = $self->resolve_parameter( '#P_ALLOW_WHITESPACE#' );
    
    my $input_quote_chararacter     = $self->resolve_parameter( '#P_INPUT_QUOTE_CHARACTER#' );
    my $input_binary                = $self->resolve_parameter( '#P_INPUT_BINARY#' );
    my $input_allow_unquoted_escape = $self->resolve_parameter( '#P_ALLOW_UNQUOTED_ESCAPE#' );
    
    my $input_delimiter             = $self->resolve_parameter( '#P_INPUT_DELIMITER#' );
    
    if ( $input_delimiter eq '\t' ) {
        $input_delimiter = "\t";
    }
    
    my $input_escape_character      = $self->resolve_parameter( '#P_INPUT_ESCAPE_CHARACTER#' );
    my $input_verbatim              = $self->resolve_parameter( '#P_INPUT_VERBATIM#' );
    
    my $input_encoding              = $self->resolve_parameter( '#P_INPUT_ENCODING#' );
    my $output_encoding             = $self->resolve_parameter( '#P_OUTPUT_ENCODING#' );
    
    $input_filename                 = $self->detokenize( $input_filename );
    $output_filename                = $self->detokenize( $output_filename );
    
    my $template_text               = $self->detokenize( $template_config->{TEMPLATE_TEXT} );

    my $validators_json             = $self->resolve_parameter( '#P_VALIDATORS#' );
    my $json_encoder                = JSON::XS->new->utf8;
    my $validators_def              = $json_encoder->decode( $validators_json );

    my %validators;

    foreach my $column ( keys %{$validators_def} ) {
        my $regex = $validators_def->{$column};
        $validators{ $column }      = qr /$regex/i;
    }

    if ( $input_encoding ) {
        $input_encoding     = ":encoding($input_encoding)";
    }
    
    if ( $output_encoding ) {
        $output_encoding    = ":encoding($output_encoding)";
    }
    
    my ( $counter, $bad_rows ) = ( 0 , 0 );
    
    eval {
        
        my ( $input, $output );
        
        {
            no warnings 'uninitialized';
            open $input, "<" . $input_encoding, $input_filename . ".headers"
                || die( "Failed to open input file [" . $input_filename . ".headers] for reading:\n" . $! );
            open $output, ">" . $output_encoding, $output_filename
                || die( "Failed to open output file [$output_filename] for writing:\n" . $! );
        }
        
        my $csv_reader = Text::CSV->new(
        {
            quote_char              => $input_quote_chararacter
          , binary                  => $input_binary
          , eol                     => $input_eol_character
          , sep_char                => $input_delimiter
          , escape_char             => $input_escape_character
          , allow_loose_quotes      => $allow_loose_quotes
          , allow_loose_escapes     => 1
          , allow_unquoted_escape   => $input_allow_unquoted_escape
          , allow_whitespace        => $allow_whitespace
          , quote_space             => 1
          , blank_is_undef          => 1
          , quote_null              => 1
          # input_verbatim is set later, after reading column headers
        } );
        
        my $headers = $csv_reader->getline( $input );
        
        close $input;
        
        {
            no warnings 'uninitialized';
            open $input, "<" . $input_encoding, $input_filename
                || die( "Failed to open input file [" . $input_filename . "] for reading:\n" . $! );
        }
        
        $csv_reader->column_names( $headers );

        if ( $input_verbatim ) {
            $csv_reader->verbatim( $input_verbatim );
        }
        
        while ( my $row = $csv_reader->getline_hr( $input ) ) {

            my $this_row_failed = 0;

            foreach my $column ( keys %validators ) {

                if ( $row->{$column} !~ $validators{$column} ) {

                    $this_row_failed ++;
                    $self->log->warn( "Dropped bad row [$bad_rows]" );

                }

            }

            if ( ! $this_row_failed ) {
                print $output $json_encoder->encode( $row ) . "\n";
                $counter ++;
                if ( $counter % 10000 == 0 ) {
                    $self->log->info( $self->comma_separated( $counter ) . " lines ..." );
                    #} elsif ( $counter > 50000 ) {
                    #    $self->log->info( $self->comma_separated( $counter ) . " lines ..." );
                }
            } else {
                $bad_rows += $this_row_failed;
            }

        }
        
        my $diag = $csv_reader->error_diag();
        
        if ( ref $diag eq 'Text::CSV::ErrorDiag' ) {
            
            my ( $diag_code, $diag_text, $diag_position ) = @{$diag};
            
            if ( $diag_code != 0 ) {
                
                $self->log->fatal( "Error parsing CSV:\n"
                                 . "      Code: [$diag_code]\n"
                                 . "      Text: [$diag_text]\n"
                                 . "  Position: [$diag_position]\n"
                                 );
                
            }
            
        } elsif ( $diag ne 'EOF - End of data in parsing input stream' ) {
            
            $self->log->fatal( "Error parsing CSV: [$diag]" );
            
        }
        
        close $output
            || die( "Failed to close output file:\n" . $! );
        
        close $input
            || die( "Failed to close input file:\n" . $! );
        
    };
    
    my $error = $@;
    
    my $end_ts = $self->log->prettyTimestamp();

    if ( $bad_rows ) {
        $template_text .= "\nDropped [$bad_rows] bad rows.";
    }

    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $self->detokenize( $template_config->{TARGET_DB_NAME} )
      , $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} )
      , $self->detokenize( $template_config->{TARGET_TABLE_NAME} )
      , $start_ts
      , $end_ts
      , $counter
      , $error
      , $template_text
      , to_json( $self->perf_stats, { pretty => 1 } )
      , $template_config->{NOTES}
    );
    
    if ( $@ ) {
        $self->log->fatal( $error );
    }
    
}

1;
