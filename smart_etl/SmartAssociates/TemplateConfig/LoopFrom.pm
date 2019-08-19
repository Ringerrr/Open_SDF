package SmartAssociates::TemplateConfig::LoopFrom;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    my $loop = $self->resolve_parameter( '#P_LOOP#' )
        || $self->log->fatal( "LoopFrom templates must define the [#P_LOOP#] parameter" );
    
    $loop = '_SDF_LOOP_' . $loop;
    
    my $iterator = SmartAssociates::Iterator->new(
        $self->globals
      , $loop
      , [ {} ]
    );
    
    $self->globals->ITERATOR( $loop, $iterator );
    
    my $end_ts = $self->log->prettyTimestamp();
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $self->detokenize( $template_config->{TARGET_DB_NAME} )
      , $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} )
      , $self->detokenize( $template_config->{TARGET_TABLE_NAME} )
      , $start_ts
      , $end_ts
      , 1
      , undef
      , $template_config->{TEMPLATE_TEXT}
      , undef
      , $template_config->{NOTES}
    );
    
}

1;
