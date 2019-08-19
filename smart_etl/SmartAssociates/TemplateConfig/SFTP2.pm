package SmartAssociates::TemplateConfig::SFTP2;

use strict;
use warnings;

use Net::SFTP::Foreign;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    my $target_host         = $self->resolve_parameter( '#P_TARGET_HOST#' )         || $self->log->fatal( "Missing param #P_TARGET_HOST#" );
    my $target_directory    = $self->resolve_parameter( '#P_TARGET_DIRECTORY#' );
    my $username            = $self->resolve_parameter( '#P_USERNAME#' );
    my $password            = $self->resolve_parameter( '#P_PASSWORD#' );
    my $public_key_path     = $self->resolve_parameter( '#P_PUBLIC_KEY_PATH#' );
    my $port                = $self->resolve_parameter( '#P_PORT#' );
    my $filename            = $self->resolve_parameter( '#P_FILENAME#' )            || $self->log->fatal( "Mising param #P_FILENAME#" );

    my $execution_log_text  = $self->resolve_parameter( $template_config->{TEMPLATE_TEXT} );

    $filename = $self->detokenize( $filename );
    
    if ( ! $username ) {
        $self->log->fatal( "Missing #P_USERNAME#. Who will we log in as?" );
    }
    
    if ( ! $password && ! $public_key_path ) {
        $self->log->info( "Missing #P_PASSWORD# combo *and* #P_PUBLIC_KEY_PATH#. You'd better have your ssh public key in the right place ..." );
    }
    
    my @auth;
    
    if ( $password ) {
        @auth = [ "password", $password ];
    } elsif ( $public_key_path ) {
        @auth = [ "key_path", $public_key_path ];
    }
    
    eval {
        
        my $sftp = Net::SFTP::Foreign->new(
            $target_host
          , port        => $port || 22
          , user        => $username
          , @auth
        ) || die( $! );
        
        if ( $target_directory ) {
            $sftp->setcwd( $target_directory )
                || die( "Unable to change directory to [$target_directory]:\n" . $sftp->error );
        }
        
        $sftp->put(
            $filename
        ) || die( "Failed to transfer file:\n" . $sftp->error );
        
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
      , $execution_log_text
      , undef
      , $template_config->{NOTES}
    );
    
    
}

1;
