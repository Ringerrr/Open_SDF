package SmartAssociates::TemplateConfig::SMTP;

use strict;
use warnings;

use MIME::Lite;
use JSON;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.2';

sub execute {
    
    my $self = shift;
    
    # This class is to parse the contents of a file using a regular expression,
    # and write the resulting columns out to a CSV. A .good and .bad file are written.
    # Records we can't parse are in the .bad file
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    my $filename                    = $self->resolve_parameter( '#P_FILENAME#' )                || $self->log->fatal( "Missing param #P_FILENAME#" );
    
    my $execution_log_text          = $self->resolve_parameter( $template_config->{TEMPLATE_TEXT} );
    
    my $email_subject               = $self->resolve_parameter( '#P_EMAIL_SUBJECT#' );
    my $email_body                  = $self->resolve_parameter( '#P_EMAIL_BODY#' );
    my $email_destination           = $self->resolve_parameter( '#P_EMAIL_DESTINATION#' )       || $ENV{'SMART_ALERT_ADDRESS'};
    my $attachment_type             = $self->resolve_parameter( '#P_ATTACHMENT_TYPE#' );
    my $email_from                  = $self->resolve_parameter( '#P_EMAIL_FROM#' )              || $ENV{'SMART_ALERT_FROM'};
    
    my $msg = MIME::Lite->new(
        From    => $email_from
      , To      => $email_destination
      , Subject => $email_subject
      , Type    => 'multipart/mixed'
    );
    
    # Add the text message part
    $msg->attach(
        Type     => 'TEXT'
      , Data     => $email_body
    );

    # Add the attachment
    $msg->attach(
        Type        => $attachment_type
      , Path        => $filename
      , Disposition => 'attachment'
    );
    
    $msg->send;
    
    my $error = $@;
    my $end_ts = $self->log->prettyTimestamp();
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , undef
      , undef
      , undef
      , $start_ts
      , $end_ts
      , 1
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
