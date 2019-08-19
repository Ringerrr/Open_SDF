package SmartAssociates::TemplateConfig::ColumnProfileReport;

use strict;
use warnings;

use Text::CSV;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    my $database                        = $self->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' );
    my $database                        = $self->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' );
    my $database                        = $self->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' );
    my $min_frequency_cutoff_percent    = $self->resolve_parameter( '#P_MIN_FREQUENCY_CUTOFF_PERCENT#' );
    my $values_limit                    = $self->resolve_parameter( '#P_VALUES_LIMIT#' );
    
    $min_frequency_cutoff_percent       =~ s/%//;
    
    $self->target_database( $self->processing_group->target_database( $template_config->{CONNECTION_NAME}, $target_db ) );
    
    my $connection   = $self->target_database;
    
    eval {
        
        my $table_fields        = $connection->fetch_field_list( $database_name, $schema_name, $table_name ); # TODO
        my @stats               = ();
        
        my $number_of_fields    = scalar @{$table_fields};
        
        my $total_records_return = $connection->select(
            "select count(*) from " . $connection->db_schema_table_string( $database_name, $schema_name, $table_name )
        );
        
        my $total_records       = $total_records_return->[0]->{COUNT};
        
        foreach my $field ( @{$table_fields} ) {
            
            if ( $field ne '_PREVIEW_ID' ) {
                
                $self->pulse( "Collecting stats on column: [$field]" );
                
                my $sql = "select    $field as VALUE, count(*) as COUNT_OF_FIELD, cast( count(*) / " . $total_records . "::FLOAT * 100 as DECIMAL(6,3) ) as VALUE_PERCENT, '$field' as PROFILED_FIELD\n"
                  . "from      " . $connection->db_schema_table_string( $database_name, $schema_name, $table_name ) . "\n"
                  . "group by  $field\n"
                  . "having    count(*) > 10\n"
                  . "order by  count(*) desc, $field\n"
                  . "limit     $values_limit";
                
                print "\n\n$sql\n\n";
                
                my $these_stats_sth = $connection->prepare( $sql );
                
                $connection->execute( $these_stats_sth );
                
                my $these_stats = $these_stats_sth->fetchall_arrayref;
                
                # [0] - 1st record ( ie most popular value )
                # [2] - 3th field - ie the percentage count vs total count
                
                if ( $these_stats->[0]->[2] > $min_frequency_cutoff_percent ) {
                    push @stats, @{$these_stats};
                } else {
                    $self->log->info( "Column [$field] most popular value [" . $these_stats->[0]->[0] . "] occurs [" . $these_stats->[0]->[1] . "]"
                       . " times, which is [" . $these_stats->[0]->[2] . "%] of the total records [$total_records]" ); 
                }
                
            }
            
        }
        
        require 'PDF/ReportWriter.pm';
        
        use constant mm     => 72/25.4;     # 25.4 mm in an inch, 72 points in an inch
        
        my $file = $self->{globals}->{paths}->{reports} . "/" . $table_name . "_column_profiler.pdf";
        unlink $file;
        
        my $fields = [
                        {
                            name                => "Value"
                          , percent             => 70
                          , align               => "left"
                        }
                      , {
                            name                => "Count"
                          , percent             => 15
                          , align               => "right"
                          , format              => {
                                                        separate_thousands  => TRUE
                                                   }
                        }
                      , {
                            name                => "Percent"
                          , percent             => 15
                          , align               => "right"
                        }
        ];
        
        my $groups = [
                        {
                            name                => "Column"
                          , reprinting_header   => TRUE
                          , data_column         => 3
                          , footer_lower_buffer => 10
                          , header              => [
                                                        {
                                                            percent             => 100,
                                                          , text                => "?"
                                                          , colour              => "blue"
                                                          , bold                => TRUE
                                                          , align               => "left"
                                                          , background          => {
                                                                                      shape   => "box"
                                                                                    , colour  => "darkgrey"
                                                                                   }
                                                          
                                                        }
                                                   ]
                          , footer              => [
                                                        {
                                                            percent             => 100,
                                                          , text                => ""
                                                          , bold                => TRUE
                                                          ,                                                       
                                                        }
                                                   ]
                        }
        ];
        
        my $page = {
            header  => [
                            {
                                text                => "Data Profiler for file [$table_name], Column filter: most popular value > " . $min_frequency_cutoff_percent . "%"
                              , colour              => "lightgreen"
                              , percent             => 100
                              , bold                => TRUE
                              , font_size           => 14
                              , align               => "right"
                              , background          => {
                                                          shape   => "box"
                                                        , colour  => "darkgrey"
                                                       }
                            }
                       ]
          , footer  => [
                            {
                                font_size           => 8
                              , text                => "Rendered on \%TIME\%"
                              , align               => 'left'
                              , bold                => FALSE
                              , percent             => 50
                            },
                            {
                                font_size           => 8
                              , text                => "Page \%PAGE\% of \%PAGES\%"
                              , align               => "right"
                              , bold                => FALSE
                              , percent             => 50
                            }
                       ]
        };
        
        my $report_def = {
                            destination                 => $file
                          , paper                       => "A4"
                          , orientation                 => "portrait",
    #                      , template                    => $self->{globals}->{reports} . "/billing_all_details_TEMPLATE.pdf"
                          , font_list                   => [ "Times" ]
                          , default_font                => "Times"
                          , default_font_size           => 11
                          , x_margin                    => 10 * mm
                          , upper_margin                => 30 * mm
                          , lower_margin                => 10 * mm
        };
        
        $self->{report} = PDF::ReportWriter->new( $report_def );
        
        my $data = {
            cell_borders            => TRUE
    #      , no_field_headers        => TRUE
          , fields                  => $fields
          , groups                  => $groups
          , page                    => $page
          , data_array              => \@stats
        };
        
        $self->{report}->render_data( $data );
        $self->{report}->save;
        
    };
    
    my $error = $@;
    
    if ( $error ) {
        
        if ( $log ) {
            print $log $error;
        }
        
        die( $error );
        
    }
    
    my $end_ts = $self->log->prettyTimestamp();
    
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
      , $template_config->{TEMPLATE_SQL}
      , undef
      , $template_config->{NOTES}
    );
    
    if ( $@ ) {
        $self->log->fatal( $error );
    }
    
}

1;
