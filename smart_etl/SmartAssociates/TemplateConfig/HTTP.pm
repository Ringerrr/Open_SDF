package SmartAssociates::TemplateConfig::HTTP;

use strict;
use warnings;

use HTTP::Request;
use LWP::UserAgent;
use JSON;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    my $target_host         = $self->resolve_parameter( '#P_TARGET_HOST#' )         || $self->log->fatal( "Missing param #P_TARGET_HOST#" );
    my $target_port         = $self->resolve_parameter( '#P_TARGET_PORT#' );
    my $request_path        = $self->resolve_parameter( '#P_REQUEST_PATH#' );
    my $request_method      = $self->resolve_parameter( '#P_REQUEST_METHOD#' );
    my $request_content     = $self->resolve_parameter( '#P_REQUEST_CONTENT#' );
    my $headers             = $self->resolve_parameter( '#P_HEADERS#' );
    my $timeout             = $self->resolve_parameter( '#P_TIMEOUT#' );
    
    my $url                 = $target_host . ":" . $target_port . $request_path;
    my $request             = HTTP::Request->new( $request_method => $url );
    my $user_agent          = LWP::UserAgent->new;
    
    my $template_details    = $self->detokenise( $template_config->{TEMPLATE_SQL} ); # this is just for our logging
    
    $user_agent->timeout( $timeout );
    
    # The rest should be in an eval{} block. We should catch errors decoding user-supplied JSON, for example
    eval {
        
        # Add headers if they exist
        if ( $headers ) {
            my $json_decoder = JSON->new;
            my $headers_hash = $json_decoder->decode( $headers );
            foreach my $header_key ( keys %{$headers_hash} ) {
                $request->header( $header_key, $headers_hash->{ $header_key } );
            }
        }
        
        if ( exists $request_content ) {
            $request->content( $request_content );
        }
        
        my $response = $user_agent->request( $request );
        
        if ( ! $response->is_success ) {
            die(
                "HTTP request returned response code: [" . $response->code . "]. Decoded content:\n"
              . $response->decoded_content
            );
        }
        
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
      , ( $error ? 0 : -1 )
      , $error
      , $template_details
      , undef
      , $template_config->{NOTES}
    );
    
}

1;
