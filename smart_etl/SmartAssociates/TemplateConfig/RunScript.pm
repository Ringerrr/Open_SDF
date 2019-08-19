package SmartAssociates::TemplateConfig::RunScript;

use strict;
use warnings;

# For Capturing STDERR, IPC etc
use IPC::Open3;
use IO::Select;

use JSON;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;
    
    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    my $template_text               = $self->detokenize( $template_config->{TEMPLATE_TEXT} );
    
    my $command                     = $self->resolve_parameter( '#P_COMMAND#' ) || $self->log->fatal( "Missing param #P_COMMAND#" );
    my $args_json                   = $self->resolve_parameter( '#P_ARGS_JSON#' );
    my $args;
    
    if ( $args_json ) {
        eval {
            $args = decode_json( $args_json );
        };
        my $err = $@;
        if ( $err ) {
            $self->log->fatal( "Failed to decode args:\n" . $err );
        }
    }
    
    # Not subject to shell expansion
    my @st = ( $command );
    
    if ( $args ) {
        push @st, @{$args};
    }
    
    # Probably won't need CMD_IN
    my $pid;
    
    # Note: inside this eval{} block, I'm calling die() for fatal errors instead of $self->log->fatal.
    # This will get caught at the end of the eval{} block, and the error we passed to die() will get returned
    # to the caller ( our parent class ), which will log the SQL executed, the fact that there was an error, and
    # *then* it will call $self->log->fatal.
    
    my $std_err = "";
    my $std_out = "";
    
    eval {
        
        no warnings 'uninitialized';
        
        $pid = open3(*CMD_IN, *CMD_OUT, *CMD_ERR, @st)
            || die( "Failure in launching $command!\n" . $! );
        
        my $exit_status = 0;
        
        $self->log->info( "Launching $command ..." );
        
        $SIG{CHLD} = sub {
            
            if ( waitpid( $pid, 0 ) > 0) {
                $exit_status = $?;
            }
            
        };
    
        $SIG{TERM} = sub {
            die( "SIGTERM received ... exiting" );
        };
        
        # Allows the child to take input on STDIN.. Again, not required, just a stub
        print CMD_IN "Howdy...\n";
        close( CMD_IN );
        
        my $selector = IO::Select->new();
        $selector->add(*CMD_ERR, *CMD_OUT);
        
        while ( my @ready = $selector->can_read() ) {
            
            foreach my $fh ( @ready ) {
                
                my $t = "";
                
                if ( fileno( $fh ) == fileno( CMD_ERR ) ) {
                    
                    $t = scalar <CMD_ERR>;
                    $std_err .= $t if $t;
                    
                } else {
                    
                    $t = scalar <CMD_OUT>;
                    $std_out.=$t if $t;
                    
                }
                
                $selector->remove( $fh ) if eof( $fh );
                
            }
            
        }
        
        if ( $exit_status && $std_err ) {
            
            die( "$command exited with status [$exit_status]\nSTDERR:\n$std_err" );
            
        }
        
        # Should be safe for Dan's logging stuff
        close( CMD_OUT );
        close( CMD_ERR );
        
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
      , -1
      , $error
      , $template_text . "\n\n" . $std_out
      , to_json( $self->perf_stats, { pretty => 1 } )
      , $template_config->{NOTES}
    );
    
    if ( $error ) {
        $self->log->fatal( $error );
    }
    
}

1;
