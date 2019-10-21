package SmartAssociates::TemplateConfig::ForkChildJob;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use JSON;

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    my $child_job_name = $self->resolve_parameter( '#P_CHILD_JOB_NAME#' )
        || $self->log->fatal( "ForkChildJob templates must define the [#P_CHILD_JOB_NAME#] parameter" );

    my $job_identifier  = $self->globals->JOB->field( &SmartAssociates::Database::Item::Job::Base::FLD_IDENTIFIER );
    my $job_args_string = $self->globals->JOB->field( &SmartAssociates::Database::Item::Job::Base::FLD_JOB_ARGS );
    my $job_args = {};

    if ( $job_args_string ) {
        $job_args = decode_json( $job_args_string );
    }

    $job_args->{ROOT_STEP_ID} = $template_config->{SEQUENCE_ORDER};

    my $job = SmartAssociates::Database::Item::Job::Base::generate(
        $self->globals
      , undef
      , {
            PROCESSING_GROUP    => $self->processing_group
          , IDENTIFIER          => "$job_identifier child task: [$child_job_name]"
          , JOB_ARGS            => $job_args
        }
    );

    if ( $job->key_value eq &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED ) {
        $self->log->fatal( "Failed to insert job. There is probably a job in the READY or RUNNING state already" );
    }

    my $pid = $self->startChildProcess(
        SmartAssociates::Base::CHILD_TYPE_JOB
      , $job->key_value
    );

    $self->globals->CHILD_JOB_TO_PID( $child_job_name , $pid );

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
