package SmartAssociates::TemplateConfig::SQL::BCP_Reader;

use strict;
use warnings;

# For Capturing STDERR, IPC etc
use IPC::Open3;
use IO::Select;

use Encode;
use Encode::Locale;

use JSON;

use base 'SmartAssociates::TemplateConfig::SQL';

use constant VERSION                            => '1.3';

# bcp / freebcp don't appear to support a bare minimum of options that you'd expect, eg:
# - quoting
# - escaping
# - column headers
#
# As a result, we have to handle this in the SQL,
# using quotename() and replace(), and we just
# skip column headers completely we don't really need them

sub execute_sql {
    
    my ( $self, $TEMPLATE_TEXT ) = @_;
    
    my $file_path           = $self->resolve_parameter( '#P_FILE_PATH#' )               || $self->log->fatal( "Missing #P_FILE_PATH#" );
    my $column_separator    = $self->resolve_parameter( '#P_COLUMN_SEPARATOR#' );
    
    my $bcp_path            = $self->resolve_parameter( '#P_BCP_PATH#' );
    my $first_row           = $self->resolve_parameter( '#P_FIRST_ROW#' );
    
    my $target_db           = $self->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )          || $self->log->fatal( "Missing CONFIG param: #CONFIG_TARGET_DB_NAME#" );
    my $target_schema       = $self->resolve_parameter( '#CONFIG_TARGET_SCHEMA_NAME#' ); # not required
    my $target_table        = $self->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )       || $self->log->fatal( "Missing CONFIG param: #CONFIG_TARGET_TABLE_NAME#" );
    
    # NP TODO Shall we add #P_ENCODING# here as with CSV Writer? The encoding is set in freetds conf, but if we need to change the encoding,
    
    my $credentials         = $self->target_database->credentials;
    
    my $host                = $credentials->{Host};
    my $port                = $credentials->{Port} || 1433;
    my $username            = $credentials->{Username};
    my $password            = $credentials->{Password};
    
    #################################################################################
    # TODO: this is TOTALLY UNTESTED in this class. It's tested in the BCP_Writer version only
    
    # Not subject to shell expansion
    # encode: assumes DBD::ODBC is compiled with unicode enabled
    my @st = ( $bcp_path
             , $target_db . '.' . $target_schema . '.' . $target_table
             , "in"
             , $file_path
             , -U => $username
             , -P => $password
             , -S => $host . ':' . $port
             , "-c",
             , -t => $column_separator
             , -e => $file_path . ".err"
             , -F => $first_row
             );
    
    
    # Probably won't need CMD_IN
    my $pid;
    my $rows = 0;
    
    # Note: inside this eval{} block, I'm calling die() for fatal errors instead of $self->log->fatal.
    # This will get caught at the end of the eval{} block, and the error we passed to die() will get returned
    # to the caller ( our parent class ), which will log the SQL executed, the fact that there was an error, and
    # *then* it will call $self->log->fatal.
    
    eval {
        
        no warnings 'uninitialized';
        
        $pid = open3(*CMD_IN, *CMD_OUT, *CMD_ERR, @st)
            || die( "Failure in launching bcp!\n" . $!);
        
        my $exit_status = 0;
        
        $self->log->info( "Launching " . $bcp_path . " ..." );
        
        # NP TODO Expand on signal handling a bit, these are just stubs.. Most of our IPC is done by the .ready file mechanism at the moment, but there's no reason we can't expand on this
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
        
        # NP TODO Interestingly, freebcp spits out some error stuff on STDOUT :/ Further parsing, testing of error conditions required
        
        my $errtxt = "";
        my $outtxt = "";
    
        while ( my @ready = $selector->can_read() ) {
            
            foreach my $fh ( @ready ) {
                
                my $t = "";
                
                if ( fileno( $fh ) == fileno( CMD_ERR ) ) {
                    
                    $t = scalar <CMD_ERR>;
                    $errtxt .= $t if $t;
                    
                } else {
                    
                    $t = scalar <CMD_OUT>;
                    $outtxt.=$t if $t;
                    
                    if ( $t && $t =~ /^(\d*) rows copied/ ) {
                        $rows = $1;
                    }
                    
                }
                
                $selector->remove( $fh ) if eof( $fh );
                
            }
            
        }
        
        if ( $exit_status && $errtxt ) {
            
            # NP TODO   The silly part.. freebcp screams out at you on stderr that there was an error.. but give the actual error message detail on stdin
            #           More analysis of different error messages needed, and which are reported on which handle 
            #print STDERR "The actual error was:\n\n$outtxt";
            
            die( "BCP exited with status [$exit_status], and error text:\n$errtxt\n\nshort error text:\n$outtxt" );       
            
        }
        
        # Should be safe for Dan's logging stuff
        close( CMD_OUT );
        close( CMD_ERR );
        
    };
    
    my $error = $@;
    
    $self->log->info( "BCP Writer has written [$rows] records" );
    
    return {
        record_count        => $rows
      , error               => $error
      , template_text       => "Called BCP with the following args:\n\n" . to_json( \@st, { pretty => 1 } )
    };
    
}

1;
