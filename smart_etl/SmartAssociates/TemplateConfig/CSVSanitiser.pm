package SmartAssociates::TemplateConfig::CSVSanitiser;

use strict;
use warnings;

use JSON;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    # First the required ones ...
    my $input_filename      = $self->resolve_parameter( '#P_INPUT_FILENAME#' )      || $self->log->fatal( "Missing param #P_INPUT_FILENAME#" );
    my $output_filename     = $self->resolve_parameter( '#P_OUTPUT_FILENAME#' )     || $self->log->fatal( "Missing param #P_OUTPUT_FILENAME#" );
    
    my $input_eol_character = $self->resolve_parameter( '#P_INPUT_EOL_CHARACTER#' );
    my $output_eol_character = $self->resolve_parameter( '#P_OUTPUT_EOL_CHARACTER#' );
    
    # Swap string literal \n
    if ( $input_eol_character eq '\n' ) {
        $input_eol_character = "\n";
    }
    
    my $allow_loose_quotes          = $self->resolve_parameter( '#P_ALLOW_LOOSE_QUOTES#' );
    my $allow_whitespace            = $self->resolve_parameter( '#P_ALLOW_WHITESPACE#' );
    
    my $input_quote_chararacter     = $self->resolve_parameter( '#P_INPUT_QUOTE_CHARACTER#' );
    my $output_quote_chararacter    = $self->resolve_parameter( '#P_OUTPUT_QUOTE_CHARACTER#' );
    
    if ( $output_quote_chararacter eq 'undef' ) {
        $output_quote_chararacter = undef;
    }
    
    my $input_delimiter             = $self->resolve_parameter( '#P_INPUT_DELIMITER#' );
    my $output_delimiter            = $self->resolve_parameter( '#P_OUTPUT_DELIMITER#' );
    
    if ( $input_delimiter eq '\t' ) {
        $input_delimiter = "\t";
    }
    
    if ( $output_delimiter eq '\t' ) {
        $output_delimiter = "\t";
    } elsif ( $output_delimiter eq '\001' ) {
        $output_delimiter = "\001";
    }
    
    my $input_escape_character      = $self->resolve_parameter( '#P_INPUT_ESCAPE_CHARACTER#' );
    my $output_escape_character     = $self->resolve_parameter( '#P_OUTPUT_ESCAPE_CHARACTER#' );
    
    my $input_encoding              = $self->resolve_parameter( '#P_INPUT_ENCODING#' );
    my $output_encoding             = $self->resolve_parameter( '#P_OUTPUT_ENCODING#' );
    
    my $mangle_my_eols              = $self->resolve_parameter( '#P_MANGLE_MY_EOLS#' );
    my $always_quote                = $self->resolve_parameter( '#P_ALWAYS_QUOTE#' );
    
    $input_filename                 = $self->detokenize( $input_filename );
    $output_filename                = $self->detokenize( $output_filename );
    
    my $template_text               = $self->detokenize( $template_config->{TEMPLATE_TEXT} );
    
    if ( $input_encoding ) {
        $input_encoding     = ":encoding($input_encoding)";
    }
    
    if ( $output_encoding ) {
        $output_encoding    = ":encoding($output_encoding)";
    }
    
    my $counter = 0;
    
    eval {
        
        my ( $input, $output );
        
        {
            no warnings 'uninitialized';
            open $input, "<" . $input_encoding, $input_filename
                || die( "Failed to open input file [$input_filename] for reading:\n" . $! );
            open $output, ">" . $output_encoding, $output_filename
                || die( "Failed to open output file [$output_filename] for writing:\n" . $! );
        }
        
        my $csv_reader = Text::CSV->new(
        {
            quote_char              => $input_quote_chararacter
          , binary                  => 1
          , eol                     => $input_eol_character
          , sep_char                => $input_delimiter
          , escape_char             => $input_escape_character
          , allow_loose_quotes      => $allow_loose_quotes
          , allow_whitespace        => $allow_whitespace
          , quote_space             => 1
          , blank_is_undef          => 1
          , quote_null              => 1
          , always_quote            => 1
        } );
        
        my $csv_writer = Text::CSV->new(
        {
            quote_char              => $output_quote_chararacter
          , binary                  => 1
          , eol                     => "\n"
          , sep_char                => $output_delimiter
          , escape_char             => $output_escape_character
          , quote_space             => 1
          , blank_is_undef          => 1
          , always_quote            => $always_quote
        } );
        
        # Optimisation: we do the $mangle_my_eols test ONCE, and decide which branch to take, instead of doing it for each record

        if ( $mangle_my_eols ) {
            
            while ( my $row = $csv_reader->getline( $input ) ) {
                
                # This is ultra lame, but Netezza can't handle seeing the \x0A sequence anywhere
                # other than the actual end of the line. It begs the question: how are you supposed to
                # get EOL characters in your data? Not via an external table, apparently ...
                # So here we're doing a regex replace of all \x0A sequences to \x0D. It's a horrible
                # thing to do, but most people won't notice ( windows clients ), and the rest will be
                # used to this kind of BS anyway
                
                foreach ( @{$row} ) {
                    no warnings 'uninitialized';
                    s/\x0A/\x0D/g;
                }
            
                $csv_writer->print( $output, $row );
                
                $counter ++;
            
                if ( $counter % 10000 == 0 ) {
                    $self->log->info( $self->comma_separated( $counter ) . " lines ..." );
                }
                
            }
            
        } else {
            
            while ( my $row = $csv_reader->getline( $input ) ) {
                
                $csv_writer->print( $output, $row );
                
                $counter ++;
            
                if ( $counter % 10000 == 0 ) {
                    $self->log->info( $self->comma_separated( $counter ) . " lines ..." );
                }
                
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
