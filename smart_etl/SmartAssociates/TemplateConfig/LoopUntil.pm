package SmartAssociates::TemplateConfig::LoopUntil;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    my $comparison_var_1    = $self->resolve_parameter( '#P_COMPARISON_VAR_1#' );
    my $comparison_var_2    = $self->resolve_parameter( '#P_COMPARISON_VAR_2#' );
    my $comparison_operator = $self->resolve_parameter( '#P_COMPARISON_OPERATOR#' );
    my $loop_name           = $self->resolve_parameter( '#P_LOOP_NAME#' ) || $self->log->fatal( "Loops ( LoopFrom and LoopUntil ) must define the [#P_LOOP_NAME#] parameter" );
    my $max_iterations      = $self->resolve_parameter( '#P_MAX_ITERATIONS#' );
    my $wait_seconds        = $self->resolve_parameter( '#P_WAIT_SECONDS#' );

    my $execution_log_text  = $self->resolve_parameter( $template_config->{TEMPLATE_TEXT} );

    $loop_name = '_SDF_LOOP_' . $loop_name;

    my $result_text;

    if ( $comparison_operator eq '==' ) {
        if ($comparison_var_1 == $comparison_var_2) {
            $result_text = "Loop exit conditions met ... exiting loop";
        } else {
            $result_text = "Loop exit conditions not met yet ... looping";
        }
    } elsif ( $comparison_operator eq '!=' ) {
        if ( $comparison_var_1 != $comparison_var_2 ) {
            $result_text = "Loop exit conditions met ... exiting loop";
        } else {
            $result_text = "Loop exit conditions not met yet ... looping";
        }
    } elsif ( $comparison_operator eq 'eq' ) {
        if ($comparison_var_1 eq $comparison_var_2) {
            $result_text = "Loop exit conditions met ... exiting loop";
        } else {
            $result_text = "Loop exit conditions not met yet ... looping";
        }
    } elsif ( $comparison_operator eq 'ne' ) {
        if ( $comparison_var_1 ne $comparison_var_2 ) {
            $result_text = "Loop exit conditions met ... exiting loop";
        } else {
            $result_text = "Loop exit conditions not met yet ... looping";
        }
    } elsif ( $comparison_operator eq '<' ) {
        if ( $comparison_var_1 < $comparison_var_2 ) {
            $result_text = "Loop exit conditions met ... exiting loop";
        } else {
            $result_text = "Loop exit conditions not met yet ... looping";
        }
    } elsif ( $comparison_operator eq '>' ) {
        if ( $comparison_var_1 > $comparison_var_2 ) {
            $result_text = "Loop exit conditions met ... exiting loop";
        } else {
            $result_text = "Loop exit conditions not met yet ... looping";
        }
    }

    my $error;

    my $iterator = $self->globals->ITERATOR( $loop_name );

    if ( $result_text eq "Loop exit conditions not met yet ... looping" ) {

        $iterator->push( {} );
        my $iteration_count = $iterator->count_items;

        if ( $iteration_count > $max_iterations ) {
            $error = "Exceeded max iterations [$max_iterations]";
        }

        if ( $wait_seconds ) {
            $result_text .= ". Waiting for [$wait_seconds] seconds ...";
            $self->log->info( $result_text );
            sleep( $wait_seconds );
        }

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
      , ( $error ? 0 : -1 )
      , $error
      , $execution_log_text . "\n\n" . $result_text
      , undef
      , $template_config->{NOTES}
    );

}

1;
