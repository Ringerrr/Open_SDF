package SmartAssociates::TemplateConfig::DataCop;

use strict;
use warnings;

use Text::CSV;
use File::Copy;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                                => '2.2';

# These constants are for reporting purposes
use constant    ISSUE_TYPE_MAX_LENGTH_EXCEEDED      => 'MAX_LENGTH_EXCEEDED';
use constant    ISSUE_TYPE_INTEGER_RANGE_EXCEEDED   => 'INTEGER_RANGE_EXCEEDED';
use constant    ISSUE_TYPE_NUMERIC_OVERFLOW         => 'NUMERIC_OVERFLOW';
use constant    ISSUE_TYPE_DATA_TYPE_MISMATCH       => 'DATA_TYPE_MISMATCH';
use constant    ISSUE_TYPE_COLUMN_COUNT             => 'COLUMN_COUNT';

# These globals are set once we have a database connection

my $byteint_max;
my $smallint_max;
my $integer_max;
my $bigint_max;

# These globals store compiled regular expressions
my $timestamp_regex               = qr /^([\d]+)([\-\/]{1})([\d]+)[\-\/]{1}([\d]+)\s[\d]+(:)?[\d]+:?[\d\.]+\s?(AM|PM)?$/i;
my $date_regex                    = qr /^([\d]+)([\-\/]){1}([\d]+)[\-\/]{1}([\d]+)$/;
my $int_starts_with_zero_regex    = qr /^0/;
my $int_only_zero_regex           = qr /^0$/;
my $int_not_leading_zero_padded   = qr /^0[\d]+$/;
my $int_regex                     = qr /^\s*(-?[\d]*)$/;
my $numeric_regex                 = qr /^-?([\d]*)\.?([\d]*)$/;

sub execute {
    
    my $self = shift;
    
    # This class is to cleanse data and write 2 streams: a 'good' stream and a 'bad' stream
    # This is for use with Postgres and other databases that can't perform this functionality themselves,
    # and instead fail if they encounter invalid data
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    # First the required ones ...
    my $filename                    = $self->resolve_parameter( '#P_FILE_NAME#' )               || $self->log->fatal( "Missing param #P_FILE_NAME#" );
    my $max_issues                  = $self->resolve_parameter( '#P_MAX_ISSUES#' );
    my $input_eol_character         = $self->resolve_parameter( '#P_INPUT_EOL_CHARACTER#' );
    my $input_binary                = $self->resolve_parameter( '#P_INPUT_BINARY#' );
    my $quote_chararacter           = $self->resolve_parameter( '#P_QUOTE_CHARACTER#' );
    my $delimiter                   = $self->resolve_parameter( '#P_DELIMITER#' );
    my $output_delimiter            = $self->resolve_parameter( '#P_OUTPUT_DELIMITER#' );
    my $escape_character            = $self->resolve_parameter( '#P_ESCAPE_CHAR#' );
    my $includes_header             = $self->resolve_parameter( '#P_INCLUDES_HEADERS#' );
    my $mangle_column_count         = $self->resolve_parameter( '#P_MANGLE_COLUMN_COUNT#' );
    my $allow_loose_quotes          = $self->resolve_parameter( '#P_ALLOW_LOOSE_QUOTES#' );
    my $global_sub_search           = $self->resolve_parameter( '#P_GLOBAL_SUB_SEARCH#' );
    my $global_sub_replace          = $self->resolve_parameter( '#P_GLOBAL_SUB_REPLACE#' );
    my $allow_unquoted_escape       = $self->resolve_parameter( '#P_ALLOW_UNQUOTED_ESCAPE#' );
    my $require_trailing_newline    = $self->resolve_parameter( '#P_REQUIRE_TRAILING_NEWLINE#' );

    my $input_encoding              = ":encoding(" . $self->resolve_parameter( '#P_INPUT_ENCODING#' ) . ")";
    my $output_encoding             = ":encoding(" . $self->resolve_parameter( '#P_OUTPUT_ENCODING#' ) . ")";
    
    my $target_db                   = $self->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' );
    my $target_schema               = $self->resolve_parameter( '#CONFIG_TARGET_SCHEMA_NAME#' );
    my $target_table                = $self->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' );
    
    if ( $input_eol_character eq '\n' ) {
        $input_eol_character = "\n"; # swap string literal \n with the new-line character
    }
    
    if ( $delimiter && $delimiter eq '\t' ) {
        $delimiter = "\t";           # swap string literal \t with the tab character
    }

    if ( $output_delimiter eq '\t' ) {
        $output_delimiter = "\t";
    }

    my $good_output_filename        = $filename . ".good";
    my $bad_output_filename         = $filename . ".bad";
    my $log_filename                = $filename . ".log";
    
    my ( $execution_log_text, $end_ts );
    
    my ( $input, $good_output, $bad_output, $log );
    
    {
            
        no warnings 'uninitialized';
        
        open $log, ">$output_encoding", $log_filename
            || die( "Failed to open log file [$log_filename] for writing:\n" . $! );
        
        if ( ! -e $filename ) {
            die( "Input file [$filename] doesn't exist!" );
        }
        
        my $open_string = "<" . $input_encoding;
        
        print $log "Opening [$filename] for input, with open string: [$open_string]\n";
        
        open $input, $open_string, $filename
            || die( "Failed to open input file [$filename] for reading:\n" . $! );
        
        $open_string = ">" . $output_encoding;
        
        print $log "Opening [$good_output_filename] for output, with open string: [$open_string]\n";
        
        open $good_output, $open_string, $good_output_filename
            || die( "Failed to open output file [$good_output_filename] for writing:\n" . $! );
        
        $self->globals->JOB->add_temporary_file( $good_output_filename );
        
        print $log "Opening [$bad_output_filename] for output, with open string: [$open_string]\n";
        
        open $bad_output, $open_string, $bad_output_filename
            || die( "Failed to open output file [$bad_output_filename] for writing:\n" . $! );
        
    }
    
    $self->target_database( $self->processing_group->target_database( $template_config->{CONNECTION_NAME}, $target_db ) );
    
    my $connection   = $self->target_database;
    
    $byteint_max  = $connection->BYTEINT_MAX;
    $smallint_max = $connection->SMALLINT_MAX;
    $integer_max  = $connection->INTEGER_MAX;
    $bigint_max   = $connection->BIGINT_MAX;
    
    my ( $row_counter, $issues_array, @validators );
    
    eval {
        
        my $csv_reader = Text::CSV->new(
        {
            quote_char              => $quote_chararacter
          , binary                  => $input_binary
          , eol                     => $input_eol_character
          , sep_char                => $delimiter
          , escape_char             => $escape_character
          , allow_loose_quotes      => $allow_loose_quotes
          , allow_unquoted_escape   => $allow_unquoted_escape
          , quote_space             => 1
          , blank_is_undef          => 1
          , quote_null              => 1
          , always_quote            => 1
        } );
        
        my $csv_writer = Text::CSV->new(
        {
            quote_char              => $quote_chararacter
          , binary                  => 0
          , eol                     => "\n"
          , sep_char                => $output_delimiter
          , escape_char             => $escape_character
          , quote_space             => 1
          , blank_is_undef          => 1
          , always_quote            => 0
        } );
        
        my $column_info = $connection->fetch_dbi_column_info(
            $target_db
          , $target_schema
          , $target_table
        );
        
        # Assemble an array of subs to call for each column type
        
        my $validation_counter = 0;
        
        foreach my $this_column_info ( @{$column_info} ) {
            
            my $db_type_code     = $this_column_info->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_DATA_TYPE ];
            my $validate_ref;
            
            # First we create a coderef to the validation sub to use ...
            
            if ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_VARCHAR
                                  , &SmartAssociates::Database::Connection::Base::SQL_WVARCHAR
                                  , &SmartAssociates::Database::Connection::Base::SQL_CHAR
                                  , &SmartAssociates::Database::Connection::Base::SQL_WCHAR ]
            ) {
                
                if ( $this_column_info->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_COLUMN_SIZE ] ) {
                    
                    $validate_ref = sub { validate_char( $this_column_info, @_ ) };
                    
                } else {
                    
                    # If we don't get a length from the database, we don't do any checking at all. Yay :)
                    
                    $validate_ref = sub {
                        return ( undef, undef );
                    };
                    
                }
                
            } elsif ( $db_type_code == &SmartAssociates::Database::Connection::Base::SQL_LONGVARCHAR ) {
                
                # For this type, we don't get a length from the database, so we don't do any checking at all. Yay :)
                
                $validate_ref = sub {
                    return ( undef, undef );
                };
                
            } elsif ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_DATETIME
                                       , &SmartAssociates::Database::Connection::Base::SQL_TIMESTAMP ]
            ) {
                
                $validate_ref = sub { validate_timestamp( $this_column_info, @_ ) };
                
            } elsif ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_DATE
                                       , &SmartAssociates::Database::Connection::Base::SQL_TYPE_DATE ]
            ) {
                
                $validate_ref = sub { validate_date( $this_column_info, @_ ) };
                
            } elsif ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_BIGINT
                                       , &SmartAssociates::Database::Connection::Base::SQL_TINYINT
                                       , &SmartAssociates::Database::Connection::Base::SQL_INTEGER
                                       , &SmartAssociates::Database::Connection::Base::SQL_SMALLINT ]
            ) {
                
                $validate_ref = sub { validate_int( $this_column_info, @_ ) };
                
            } elsif ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_NUMERIC
                                       , &SmartAssociates::Database::Connection::Base::SQL_DECIMAL ]
            ) {
                
                $validate_ref = sub { validate_numeric( $this_column_info, @_ ) };
                
            } else {
                
                my $sql_type = $this_column_info->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_SQL_DATA_TYPE ];
                    
                die( "Encountered DB Type Code [$sql_type] [$db_type_code], which is currently not handled yet.\n"
                   . "Check the SQL_* constants in SmartAssociates::Database::Connection::Base and make sure they're"
                   . " handled in SmartAssociates::TemplateConfig::DataCop" );
                
            }
            
            # Now attach the code ref to the current definition
            $validators[ $validation_counter ] = $validate_ref;
            
            $validation_counter ++;
            
        }
        
        ########################################################
        
        my $db_column_count = scalar @{$column_info};
        
        my ( $column_count_memory , $extra_columns );

        $column_count_memory = 0;

        if ( $includes_header ) {
            
            my $header_row = $csv_reader->getline( $input );
            
            if ( defined $header_row ) { # We can get passed a zero-byte file. Don't die on this ...
                
                $row_counter ++;
                
                $column_count_memory = scalar @{$header_row};
                
                if ( $column_count_memory > $db_column_count ) {
                    
                    # This will hopefully mainly happen when people open a file in excel, frig around, add some columns,
                    # and save the file back to csv. While this shouldn't be happening, the least painful way to deal with
                    # it is to log a warning here, then strip the excess columns ( including in headers ) here, then proceed
                    # as if everything's dandy
                    
                    $self->log->warn( "The current file [$filename] has [$column_count_memory] columns, but the target"
                                    . " database table has only [$db_column_count] columns. Extra columns will be STRIPPED." );
                    
                    for my $chomp_counter ( $db_column_count + 1 .. $column_count_memory ) {
                        pop @{$header_row};
                    }
                    
                    # Finally, remember the number of columns to pop ...
                    $extra_columns = $column_count_memory - $db_column_count;
                    
                }
                
            } else {
                
                $self->log->warn( "The current file [$filename] doesn't contain any data" );
                
            }
            
            $csv_writer->print( $good_output, $header_row );
            $csv_writer->print( $bad_output,  $header_row );
            
        }
        
        my ( $validated_records , $rejected_records , $issues_count ) = ( 0 , 0 , 0 );
        
        while ( my $row = $csv_reader->getline( $input ) ) {
            
            $row_counter ++;
            my @errors;
            
            my $this_column_count = scalar @{$row};
            
            if ( $this_column_count != $column_count_memory ) {
                
                if ( ! $column_count_memory ) { # optimisation - we don't want to run this check for each record, so only do it if there is a count mismatch
                    
                    $column_count_memory = scalar @{$row};
                    
                } elsif ( $mangle_column_count ) {
                    
                    if ( $this_column_count > $column_count_memory ) {
                        
                        for ( $column_count_memory + 1 .. $this_column_count ) {
                            pop @{$row}; # Who cares about extra columns anyway?
                        }
                        
                    } else {
                        
                        for ( $this_column_count + 1 .. $column_count_memory ) {
                            push @{$row}, undef; # This will "fix" things ...
                        }
                        
                    }
                    
                } else {
                    
                    $self->log->warn( "Column count changed from [$column_count_memory] to " . scalar @{$row} );

                    push @errors, "Row:             [$row_counter]\n"
                                . "Error:           [" . ISSUE_TYPE_COLUMN_COUNT() . "]";

                    $issues_array->[ -1 ]->{ ISSUE_TYPE_COLUMN_COUNT() } ++;

#                    $column_count_memory = scalar @{$row}; # ??? This would set our column count to the number in the record we just rejected
                    
                }
                
            }
            
            # Compare each value
            
            my $column_position = 0;
            
            foreach my $data ( @{$row} ) {

                if ( $global_sub_search ) {
                    no warnings 'uninitialized';
                    $data =~ s/$global_sub_search/$global_sub_replace/g;
                }

                if ( $column_position >= $db_column_count ) { # $column_position starts at 0
                    
                    # Using 'pop' here appears to corrupt our 'foreach' loop :/ Do nothing now, and pop after the loop completes
                    #pop @{$row};
                    
                } elsif ( defined $data ) { # TODO: check NULLABLE state
                    
                    # call validate code
                    my ( $this_error, $this_error_code ) = $validators[ $column_position ]( $data );
                    
                    if ( $this_error ) {
                        
                        my $column_def          = $column_info->[ $column_position ];
                        
                        push @errors, "Row:             [$row_counter]\n"
                                    . "Column Position: [$column_position]\n"
                                    . "Column Name:     [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_COLUMN_NAME ] . "]\n"
                                    . "Data Item:       [" . $data . "]\n\n$this_error";
                        
                        $issues_array->[ $column_position ]->{ $this_error_code } ++;
                        
                        $issues_count ++;
                        
                    }
                    
                }
                
                $column_position ++;
                
            }
            
            if ( $extra_columns ) {
                
                for my $pop_counter ( 1 .. $extra_columns ) {
                    pop @{$row};
                }
                
            }
            
            if ( @errors ) {
                
                $rejected_records ++;
                
                $csv_writer->print( $bad_output, $row );
                
                my $msg = "=================================================================================================\n\n"
                         . ( join( "\n\n", @errors ) ) . "\n"
                         . "This will be record [" . ( $rejected_records + 1 ) . "] ( including the header line ) in the file [$bad_output_filename]\n\n"; # +1 for the header
                
                print $log $msg;
                $self->log->warn( $msg ); 
                
            } else {
                
                $validated_records ++;
                
                $csv_writer->print( $good_output, $row );
                
            }
        
            if ( $row_counter % 10000 == 0 ) {
                $self->log->info( $self->comma_separated( $row_counter ) . " lines ..." );
            }
            
        }
        
        my $diag = $csv_reader->error_diag();
        
        if ( ref $diag eq 'Text::CSV::ErrorDiag' ) {
            
            my ( $diag_code, $diag_text, $diag_position ) = @{$diag};
            
            if ( $diag_code != 0 ) {

                if ( $diag_code == 2012 ) {
                    my $err_text = "Error parsing CSV:\n"
                                 . "      Code: [$diag_code]\n"
                                 . "      Text: [$diag_text]\n"
                                 . "  Position: [$diag_position]\n\n"
                                 . "After reading [$row_counter] lines of the file";
                    if ( $require_trailing_newline ) {
                        $self->log->fatal( $err_text );
                    } else {
                        $self->log->warn( $err_text );
                    }
                }
                
            }
            
        } elsif ( $diag ne 'EOF - End of data in parsing input stream' ) {
            
            $self->log->fatal( "Error parsing CSV: [$diag]" );
            
        }
        
        if ( $row_counter % 10000 != 0 ) {
            print $log "$row_counter lines ...\n";
            $self->log->info( $self->comma_separated( $row_counter ) . " lines" );
        }
        
        # Write totals to log db
        my $log_dbh = $self->globals->JOB->dbh;
        
        $end_ts = $self->log->prettyTimestamp();
        
        # Calculate the seconds taken - we put this in the execution log text, so we can provide a 'values per second' metric
        my $sth = $log_dbh->prepare(
            "select extract( 'epoch' from '" . $end_ts . "'::TIMESTAMP - '" . $start_ts . "'::TIMESTAMP ) as SECONDS"
        );
        
        $log_dbh->execute( $sth );
        
        my $seconds_rec = $sth->fetchrow_arrayref;
        my $seconds = $seconds_rec->[0];
        
        $sth = $log_dbh->prepare(
            "insert into data_cop_totals\n"
          . "(\n"
          . "    job_id\n"
          . "  , database_name\n"
          . "  , schema_name\n"
          . "  , table_name\n"
          . "  , records_validated\n"
          . "  , records_rejected\n"
          . ") values (\n"
          . "    ? , ? , ? , ? , ? , ?\n"
          . ")"
        );
        
        $log_dbh->execute(
            $sth
          , [
                $self->resolve_parameter( '#ENV_JOB_ID#' )
              , $target_db
              , $target_schema
              , $target_table
              , $validated_records
              , $rejected_records
            ]
        );
        
        $execution_log_text = "Processed [$row_counter] records of [$column_count_memory] columns ( [" . ( $row_counter * $column_count_memory )
            . "] values ) in [$seconds] seconds ...\n"
            . "  " . int( $row_counter / ( $seconds == 0 ? 0.001 : $seconds ) ) . " records per second\n"
            . "  " . int( ( ( $row_counter * $column_count_memory ) / ( $seconds == 0 ? 0.001 : $seconds ) ) ) . " values per second";
        
        if ( $issues_array ) {
            
            $sth = $log_dbh->prepare(
                "insert into data_cop_issues_summary\n"
              . "(\n"
              . "    job_id\n"
              . "  , database_name\n"
              . "  , schema_name\n"
              . "  , table_name\n"
              . "  , column_name\n"
              . "  , column_type\n"
              . "  , column_type_extra\n"
              . "  , issue_type\n"
              . "  , issue_count\n"
              . ") values (\n"
              . "    ? , ? , ? , ? , ? , ? , ? , ? , ?\n"
              . ")"
            );
            
            $execution_log_text .= "\n\nSummary of issues encountered:\n"; 
            
            $self->log->warn( $execution_log_text );
            
            my $column_position = 0;
            
            foreach my $column_details ( @{$issues_array} ) {
                
                if ( $column_details ) {
                    
                    my ( $column_name , $column_type , $column_type_extra );
                    
                    my $column_def = $column_info->[$column_position];
                    
                    $column_name = $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_COLUMN_NAME ];
                    $column_type = $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_TYPE_NAME ];
                    
                    $execution_log_text .= "=========================================================\n\n" 
                                         . "Column #:  [$column_position]\n"
                                         . "Name:      [$column_name]\n"
                                         . "Type:      [$column_type]\n";
                    
                    my $db_type_code = $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_DATA_TYPE ];
                    
                    if ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_BIGINT
                                          , &SmartAssociates::Database::Connection::Base::SQL_TINYINT
                                          , &SmartAssociates::Database::Connection::Base::SQL_INTEGER
                                          , &SmartAssociates::Database::Connection::Base::SQL_SMALLINT ] ) {
                        
                        $execution_log_text .= "SubType:   [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_INT_SUBTYPE ] . "]\n";
                        
                        $column_type_extra = "Size: [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_INT_SUBTYPE ] . "]";
                        
                    } elsif ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_NUMERIC
                                               , &SmartAssociates::Database::Connection::Base::SQL_DECIMAL ] ) {
                            
                        $execution_log_text .= "Scale:     [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_SCALE ] . "]\n\n"
                                             . "Precision: [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_PRECICION ] . "]\n";
                        
                        $column_type_extra = "Scale: [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_SCALE ] . "], "
                                           . "Precision: [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_PRECICION ] . "]";
                        
                    } elsif ( $db_type_code ~~ [ &SmartAssociates::Database::Connection::Base::SQL_VARCHAR
                                               , &SmartAssociates::Database::Connection::Base::SQL_WVARCHAR
                                               , &SmartAssociates::Database::Connection::Base::SQL_CHAR
                                               , &SmartAssociates::Database::Connection::Base::SQL_WCHAR ] ) {
                            
                        $execution_log_text .= "Length:    [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_COLUMN_SIZE ] . "]\n";
                        
                        $column_type_extra = "Length: [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_COLUMN_SIZE ] . "]";
                        
                    }
                    
                    $execution_log_text .= "\n";
                    
                    while ( my ( $issue_type, $issue_count ) = each ( %{$column_details} ) ) {
                        
                        $execution_log_text .= " $issue_type\n   occurred [$issue_count] times\n";
                        
                        $log_dbh->execute(
                            $sth
                          , [
                                $self->resolve_parameter( '#ENV_JOB_ID#' )
                              , $target_db
                              , $target_schema
                              , $target_table
                              , $column_name
                              , $column_type
                              , $column_type_extra
                              , $issue_type
                              , $issue_count
                            ]
                        );
                        
                    }
                    
                }
                
                $column_position ++;
                
            }
            
            print $log $execution_log_text . "\n";
            $self->log->warn( $execution_log_text );
            
        } else {
            
            print $log "\n\nNo issues encountered.\n";
            
            $self->log->info( "No issues encountered" );
            
        }
        
        close $good_output
            || die( "Failed to close output file:\n" . $! );
        
        close $bad_output
            || die( "Failed to close output file:\n" . $! );
        
        close $input
            || die( "Failed to close input file:\n" . $! );
        
        close $log
            || die( "Failed to close log file:\n" . $! );
        
    };
    
    if ( ! $issues_array ) {
        
        unlink $bad_output_filename;
        unlink $log_filename;
        
    } else {
        my $log_dir = $self->resolve_parameter( '#ENV_LOG_DIR#' );
        
        move( $bad_output_filename
            , $log_dir )
                || $self->log->warn( "Failed to move [$bad_output_filename] to [$log_dir]\n" . $! );
        
        move( $log_filename
            , $log_dir )
                || $self->log->warn( "Failed to move [$log_filename] to [$log_dir]\n" . $! );
        
    }
    
    my $error = $@;
    
    if ( $error ) {
        
        if ( $log ) {
            print $log $error;
        }
        
        die( $error );
        
    }
    
    if ( $issues_count > $max_issues ) {
        if ( $error ) {
            $error .= "\n";
        }
        $error .= "Max issues count [$max_issues] exceeded ... detected [$issues_count] issues";
    }
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $self->detokenize( $template_config->{TARGET_DB_NAME} )
      , $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} )
      , $self->detokenize( $template_config->{TARGET_TABLE_NAME} )
      , $start_ts
      , $end_ts
      , $row_counter
      , $error
      , $execution_log_text
      , undef
      , $template_config->{NOTES}
    );
    
    if ( $@ ) {
        $self->log->fatal( $error );
    }
    
}

sub validate_char {
                    
    my ( $column_def, $data ) = @_;
    
    my ( $this_error, $this_error_code );
    
    my $length = length( $data ) || 1;
    
    if ( $length > $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_COLUMN_SIZE ] ) {
                            
        $this_error = "Detected data of length [$length],\n"
                    . "but in the DB this is defined as a CHAR type with max length [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_COLUMN_SIZE ] . "].\n"
                    . "Max length exceeded.";
        
        $this_error_code = ISSUE_TYPE_MAX_LENGTH_EXCEEDED();
        
    }
    
    return ( $this_error, $this_error_code );
    
}

sub validate_timestamp {
    
    my ( $column_def, $data ) = @_;
    
    my ( $this_error, $this_error_code );
    
    if ( $data !~ $timestamp_regex && $data !~ $date_regex ) {
        
        my $type = detect_type( $data );
        
        $this_error = "Detected contains data of type [$type],\n"
                    . "but in the DB this is defined as a SQL_DATETIME type.\n"
                    . "Incompatible type.";
        
        $this_error_code = ISSUE_TYPE_DATA_TYPE_MISMATCH();
        
    }
    
    return ( $this_error, $this_error_code );
    
}

sub validate_date {
    
    my ( $column_def, $data ) = @_;
    
    my ( $this_error, $this_error_code );
    
    if ( $data !~ $date_regex ) {
        
        my $type = detect_type( $data );
        
        $this_error = "Detected data of type [$type],\n"
                    . "but in the DB this is defined as a SQL_DATE / SQL_TYPE_DATE type.\n"
                    . "Incompatible type.";
        
        $this_error_code = ISSUE_TYPE_DATA_TYPE_MISMATCH();
        
    }
    
    return ( $this_error, $this_error_code );
    
}

sub validate_int {
    
    my ( $column_def, $data ) = @_;
    
    my ( $this_error, $this_error_code );
    
    my $subtype;
    
    if (
            (
                $data !~ $int_not_leading_zero_padded   # We don't want to match things like 00030 ...
            )
          &&    $data =~ $int_regex
    ) {
        
        my $number = $1;
        
        # avoid warnings, and do proper numeric:numeric comparisons ...
        if ( $number eq '' ) {
            $number = 0;
        }
        
        if (      $number > -$byteint_max  && $number < $byteint_max ) {
            
            $subtype = &SmartAssociates::Database::Connection::Base::BYTEINT;
            
        } elsif ( $number > -$smallint_max && $number < $smallint_max ) {
            
            $subtype = &SmartAssociates::Database::Connection::Base::SMALLINT;
            
        } elsif ( $number > -$integer_max  && $number < $integer_max ) {
            
            $subtype = &SmartAssociates::Database::Connection::Base::INT;
            
        } elsif ( $number > -$bigint_max   && $number < $bigint_max ) { # can't use string literals directly, apparently
            
            $subtype = &SmartAssociates::Database::Connection::Base::BIGINT;
            
        } else {
            
            $this_error = "Detected data of type [VARCHAR] ( integer that exceeded the BIGINT max ),\n"
                        . "but in the DB this is defined as an SQL_DATE / SQL_TYPE_DATE type.\n"
                        . "Incompatible type.";
            
            $this_error_code = ISSUE_TYPE_DATA_TYPE_MISMATCH();
            
        }
        
        # If we're here, we DO have an integer, and we also have a sub-type. Check the detected sub-type against the DB's
        
        if ( $subtype > $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_INT_SUBTYPE ] ) {
            
            $this_error = "Detected data of type [INTEGER], subtype [$subtype],\n"
                        . "but in the DB this is defined as INTEGER, subtype [$subtype].\nInteger overflow.";
            
            $this_error_code = ISSUE_TYPE_NUMERIC_OVERFLOW();
            
        }
        
    } else {
        
        # If we're here, we've got something non-integer
        my $type = detect_type( $data );
        
        $this_error = "Detected data of type [$type],\n"
                    . "but in the DB this is defined as INTEGER, subtype [$subtype].\nIncompatible type.";
        
        $this_error_code = ISSUE_TYPE_DATA_TYPE_MISMATCH();
        
    }
    
    return ( $this_error, $this_error_code );
    
}

sub validate_numeric {
    
    my ( $column_def, $data ) = @_;
    
    my ( $this_error, $this_error_code );
    
    if ( $data =~ $numeric_regex ) {
        
        my ( $left, $right ) = ( $1, $2 );
        
        my $left_digits  = length( $left );
        my $right_digits = length( $right );
        my $scale        = $left_digits + $right_digits;
        my $precision    = $right_digits;
        
        no warnings 'uninitialized';
        
        if ( $scale     > $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_SCALE ]
          || $precision > $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_PRECICION ]
        ) {
            
            $this_error = "Detected data of type [NUMERIC], scale [$scale], precision [$precision],\n"
                        . "but in the DB this is defined as "
                        . "SQL_NUMERIC/SQL_DECIMAL, scale [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_SCALE ] . "]"
                        . ", precision [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_PRECICION ] . "]"
                        . "\nNumeric overflow.";
            
            $this_error_code = ISSUE_TYPE_NUMERIC_OVERFLOW();
            
        }
        
    } else {
        
        my $type = detect_type( $data );
        
        $this_error = "Detected data of type [$type],\n"
                    . "but in the DB this is defined as SQL_NUMERIC/SQL_DECIMAL, scale [" . $column_def->[ &SmartAssociates::Database::Connection::Base::COLUMN_INFO_SCALE ] . "]\n"
                    . "Incompatible type.";
        
        $this_error_code = ISSUE_TYPE_DATA_TYPE_MISMATCH();
        
    }
    
    return ( $this_error, $this_error_code );
    
}

sub detect_type {
    
    my $data = shift;
    
    return "unknown";
    
}

1;
