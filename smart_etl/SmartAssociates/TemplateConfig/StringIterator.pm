package SmartAssociates::TemplateConfig::StringIterator;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.1';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    my $delimiter   = $self->resolve_parameter( '#P_DELIMITER#' );
    
    my $p_string    = $self->resolve_parameter( '#P_STRING#') ;
    
    my $iterator_store_name = $self->resolve_parameter( '#P_ITERATOR#' )
        || $self->log->fatal( "Iterators must define the [#P_ITERATOR#] parameter" );
    
    my $counter;
    
    my @columns;
    my $all_records;
    
    eval {
        
        my @lines       = split( /\n/, $p_string );
        
        foreach my $line ( @lines ) {
            
            if ( ! $counter ) {
                
                @columns = split( /$delimiter/, $line );
                
                foreach my $column ( @columns ) {
                    $column =~ s/\s//g;                                 # strip spaces
                }
                
            } else {
                
                my $record;
                my @array = split( /$delimiter/, $line );
                
                foreach my $col_counter ( 0 .. @array - 1 ) {
                    my $value = $array[ $col_counter ];
                    $value =~ s/(^\s*|\s*$)//g;                         # strip spaces
                    $record->{ $columns[ $col_counter ] } = $value;
                }
                
                push @{$all_records}, $record;
            }
            
            $counter ++;
            
        }
        
    };
    
    my $error = $@;
    
    my $iterator = SmartAssociates::Iterator->new(
        $self->globals
      , $iterator_store_name
      , $all_records
    );
    
    $self->globals->ITERATOR( $iterator_store_name, $iterator );
    
    my $end_ts = $self->log->prettyTimestamp();
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $self->detokenize( $template_config->{TARGET_DB_NAME} )
      , $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} )
      , $self->detokenize( $template_config->{TARGET_TABLE_NAME} )
      , $start_ts
      , $end_ts
      , $counter - 1 # don't count the column headings
      , $error
      , $template_config->{TEMPLATE_TEXT}
      , undef
      , $template_config->{NOTES}
    );
    
    if ( $error ) {
        $self->log->fatal( $error );
    }
    
}

1;
