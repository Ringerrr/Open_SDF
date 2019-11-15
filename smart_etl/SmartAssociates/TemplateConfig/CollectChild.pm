package SmartAssociates::TemplateConfig::CollectChild;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    my $child_job_name = $self->resolve_parameter( '#P_CHILD_JOB_NAME#' )
        || $self->log->fatal( "CollectChild templates must define the [#P_CHILD_JOB_NAME#] parameter" );

    my $pid = $self->globals->CHILD_JOB_TO_PID( $child_job_name );

    $self->log->info( "Found PID: [$pid] for child job name: [$child_job_name]. Waiting ..." );

    $self->captureExitingChildByPID( $pid );

    my $pid_to_job_id_mapping = $self->globals->PID_TO_JOB_ID_MAPPING();

    my $child_job = SmartAssociates::Database::Item::Job::Base::generate(
        $self->globals
      , $pid_to_job_id_mapping->{ $pid }
    );

    my $error_message;

    my $child_job_exit_status = $child_job->field( &SmartAssociates::Database::Item::Job::Base::FLD_STATUS );
    my $notes = "Child job exit status was: [$child_job_exit_status]";

    if (  $child_job_exit_status ne &SmartAssociates::Database::Item::Job::Base::STATUS_COMPLETE ) {
        $error_message = $child_job->field( &SmartAssociates::Database::Item::Job::Base::FLD_ERROR_MESSAGE );
        if ( ! defined $error_message || $error_message eq '' ) {
            $error_message = 'Unknown ( no error registered in job_ctl )';
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
      , 1
      , $error_message
      , $template_config->{TEMPLATE_TEXT}
      , undef
      , $notes # $template_config->{NOTES}
    );

    if ( defined $error_message ) {
        $self->log->fatal( $error_message ); # The logger takes care of rolling back target databases
    }

}

# sub handleChildError {
#
#     my ( $self, $pid , $job ) = @_;
#
#     # Note: we MUST have the leading ampersand here, to force these constants to resolve at run-time
#
#     $self->log->info( "In SmartAssociates::TemplateConfig::CollectChild::captureChildError() ..." );
#
#     my $status = $job->field( &SmartAssociates::Database::Item::Job::Base::FLD_STATUS );
#     my $error_message;
#
#     if ( $status eq  &SmartAssociates::Database::Item::Job::Base::STATUS_COMPLETE ) {
#         $job->field( &SmartAssociates::Database::Item::Job::Base::FLD_STATUS, &SmartAssociates::Database::Item::Job::Base::STATUS_UNHANDLED_ERROR ); # Only set the status to 'unhandled error' if it's currently 'complete'
#         $job->update();
#     }
#
#     $error_message = $job->field( &SmartAssociates::Database::Item::Job::Base::FLD_STATUS, &SmartAssociates::Database::Item::Job::Base::FLD_ERROR_MESSAGE );
#
#     return $error_message;
#
# }

1;
