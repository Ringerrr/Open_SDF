package SmartAssociates::Log;

use strict;
use warnings;

use Carp;

use File::Path qw ' make_path ';
use Mail::Sendmail;

use base 'SmartAssociates::Base';

# These constants define logging levels
# Using integers allows us to do a simple numeric comparison to determine
# whether a log message ( at a given level ) should be printed
# to the console ( considering the global log level )

# NOTE: These log levels are also defined in metadata-gui
use constant        LOG_LEVEL_FATAL             => 0;
use constant        LOG_LEVEL_ERROR             => 1;
use constant        LOG_LEVEL_WARN              => 2;
use constant        LOG_LEVEL_INFO              => 3;
use constant        LOG_LEVEL_DEBUG             => 4;

my $IDX_LOG_LEVEL                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_LOG_DIR                                 =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_LOGHANDLE                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_LOG_LEVEL_TEXT                          =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 3;
my $IDX_LOG_PATH                                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 4;
my $IDX_WARNINGS                                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 5;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 6;

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1] );
    
    my $options = $_[2];
    
    # Build the log path based
    my $log_path = 
          $self->globals->LOGDIR;
    
    # TODO: 'mkdir -p' on logdir before continuing ... but it has to work on Windows too 
    if ( $options->{log_subdir} ) {
        $log_path .= $self->globals->DIR_SEPARATOR . $options->{log_subdir};
    }
    
    if ( ! -d $log_path ) {
        eval { make_path( $log_path ) };
        if ( $@ ) {
          die ( "Log directory [$log_path] doesn't exist, and attempt to create it failed:\n" . $@ );
        }
    }
    
    if ( ! $options->{log_type} ) {
        $options->{log_type} = "unknown";
    }
    
    $self->[ $IDX_LOG_DIR ] = $log_path;
    
    # Set the log level
    my $log_level;
    my $log_level_text = $options->{log_level_text};
    
    if (      $log_level_text eq 'fatal' ) {
        $log_level   = LOG_LEVEL_FATAL;
    } elsif ( $log_level_text eq 'error' ) {
        $log_level   = LOG_LEVEL_ERROR;
    } elsif ( $log_level_text eq 'warn' ) {
        $log_level   = LOG_LEVEL_WARN;
    } elsif ( $log_level_text eq 'info' ) {
        $log_level   = LOG_LEVEL_INFO;
    } elsif ( $log_level_text eq 'debug' ) {
        $log_level   = LOG_LEVEL_DEBUG;
    } else {
        print "I was passed an unknown --log-level value: [$log_level_text]. Defaulting to [debug]";
        $log_level   = LOG_LEVEL_DEBUG;
    }
    
    $self->[ $IDX_LOG_LEVEL ] = $log_level;
    $self->[ $IDX_LOG_LEVEL_TEXT ] = $log_level_text;
    
    $options->{log_type} =~ s/:/_/g;
    
    $self->[ $IDX_LOG_PATH ] =  $self->[ $IDX_LOG_DIR ]
                              . $self->globals->DIR_SEPARATOR
                              . $options->{log_type}
                              .'_'
                              . $self->prettyTimestamp()
                              . '.log';
	
    # Create the log handle
    open( $self->[ $IDX_LOGHANDLE ], '>', $self->[ $IDX_LOG_PATH ] )
        || die( "Could not open logfile:\n$!" );
    
    # Disable output buffering ( so we can see log messages appearing immediately )
    $self->[ $IDX_LOGHANDLE ]->autoflush( 1 );

    $self->[ $IDX_WARNINGS ] = [];

    return $self;
    
}

sub timestamp {
    
    # This function returns the current time as a standard DB kinda string
    
    my $self = shift;
    
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
    
    #               print mask ... see sprintf
    return sprintf( "%04d-%02d-%02d %02d:%02d:%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );
    
}

sub date {
    
    my $self = shift;
    
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
    
    #               print mask ... see sprintf
    return sprintf( "%04d-%02d-%02d", $year + 1900, $mon + 1, $mday );
    
}

sub prettyTimestamp {
    
    # This function returns the current time as a human-readable timestamp
    #  ... without having to install further date/time manipulation libraries
    
    my $self = shift;
    
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
    
    #               print mask ... see sprintf
    return sprintf( "%04d%02d%02d_%02d%02d%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );
    
}

sub logLine {
    
    # This function logs a given string to the logfile
    # $level is the severity level ( eg info, error, etc )
    # $text is the string to log
    
    my ( $self, $level, $text, $override_source_line_number, $override_filename ) = @_;
    
    # our ( $LOG_LEVEL, $LOGHANDLE, $JOB_ID );
    
    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )
    
    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );
    
    # Allow the caller to override the line number that we log ...
    if ( $override_source_line_number ) {
        $line = $override_source_line_number;
    }
    
    # Allow the caller to override the filename that we log ...
    if ( $override_filename ) {
        $filename = $override_filename;
    }
    
    # Are we the batch or are we a job?
    my $job_batch_set =
        (   defined $self->globals->JOB   ? $self->globals->JOB->key_value
          : defined $self->globals->BATCH ? 'B'
          : 'S'
        );
    
    # TODO: what's going on here? Can't print direct to $self->[ $IDX_LOGHANDLE ]?
    my $log_handle = $self->[ $IDX_LOGHANDLE ];
    
    # We always log to the detail log ( ie $LOGHANDLE )
    print $log_handle $self->prettyTimestamp()
     . " " . $level . " " . $job_batch_set . " " . $filename . ":" . $line . " " . $text . "\n";
    
    # ... and then we only log to the console if the severity of the message we've been passed is under the cutoff
    # ( ie we don't log debugging messages unless we're running in debug mode )
    if ( $level <= $self->[ $IDX_LOG_LEVEL ] ) {
        print $self->prettyTimestamp() . " " . $level . " " . $job_batch_set . " " . $filename . ":" . $line . " " . $text . "\n";
    }
    
}

sub debug {
    
    my ( $self, $text, $override_source_line_number, $override_filename ) = @_;
    
    $self->logLine( LOG_LEVEL_DEBUG, $text, $override_source_line_number, $override_filename );
    
}

sub info {
    
    my ( $self, $text, $override_source_line_number, $override_filename ) = @_;
    
    $self->logLine( LOG_LEVEL_INFO, $text, $override_source_line_number, $override_filename );
    
}

sub warn {
    
    my ( $self, $text, $override_source_line_number, $override_filename ) = @_;
    
    $self->logLine( LOG_LEVEL_WARN, $text, $override_source_line_number, $override_filename );

    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )

    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );

    # Allow the caller to override the line number that we log ...
    if ( $override_source_line_number ) {
        $line = $override_source_line_number;
    }

    # Allow the caller to override the filename that we log ...
    if ( $override_filename ) {
        $filename = $override_filename;
    }

    my $warning_hash = {
        location => $filename
      , line     => $line
      , warning  => $text
    };

    push @{ $self->[ $IDX_WARNINGS ] } , $warning_hash;

}

sub warnings {

    my $self = shift;

    return $self->[ $IDX_WARNINGS];

}

sub clear_warnings {

    my $self = shift;

    $self->[ $IDX_WARNINGS ] = [];

}

sub error {
    
    my ( $self, $text, $override_source_line_number, $override_filename ) = @_;
    
    $self->logLine( LOG_LEVEL_ERROR, $text, $override_source_line_number, $override_filename );
    
}

sub fatal {
    
    my ( $self, $text, $override_source_line_number, $override_filename ) = @_;
    
    my $current_template_config = $self->globals->CURRENT_TEMPLATE_CONFIG;
    
    if ( $current_template_config && $current_template_config->on_error_continue ) {
        $self->log->info( "Log->fatal() called, but current config has ON_ERROR_CONTINUE flag set. Downgrading error and continuing ..." );
        $self->logLine( LOG_LEVEL_ERROR, $text, $override_source_line_number, $override_filename );
        die( "Current config dying. This should be caught by ProcessingGroup::Base ( main loop ) or TemplateConfig::Base ( iterator loop )" );
    }
    
    $self->logLine( LOG_LEVEL_FATAL, $text, $override_source_line_number, $override_filename );
    
    my $misc = $self->globals->MISC;
    $misc->{FATAL_PARACHUTE} ++;
    $self->globals->MISC( $misc );
    
    my ( $batch, $job );
    
    eval { # Trap any errors in this block
        
        $job = $self->globals->JOB;
        
        if ( defined $job ) {
            
            # We're a worker with a job. Update the status.
            
            # These eval blocks is just to trap an errors in disconnecting
            # and rolling back, which we don't really care about at this point
            # ( we're already in an error state )
            
            $self->globals->CONFIG_GROUP->rollback_target_databases();
            
            # If things go really sour with Netezza, we can get into a infinite loop, calling logFatal().
            # To protect from this, we check $FATAL_PARACHUTE. If it's been incremented past 1,
            # we avoid further DB updates.
            
            my $misc = $self->globals->MISC;
            
            if ( $misc->{FATAL_PARACHUTE} == 1 ) {
                
                my $job = $self->globals->JOB;
                
                if ( $job ) {
                    
                    $job->field(
                        SmartAssociates::Database::Item::Job::Base::FLD_STATUS
                      , SmartAssociates::Database::Item::Job::Base::STATUS_ERROR
                    );
                    
                    $job->field(
                        SmartAssociates::Database::Item::Job::Base::FLD_ERROR_MESSAGE
                      , substr( $text, 0, 20000 )
                    );
                    
                    $job->update();
                }
                
            }
            
        }
        
        $batch = $self->globals->BATCH;
        
        if ( defined $batch ) {
            $batch->field( SmartAssociates::Database::Item::Batch::Base::FLD_STATUS, SmartAssociates::Database::Item::Batch::Base::STATUS_ERROR );
            $batch->update();
        }
        
    };
    
    my $tmp_files = $self->globals->TMP_FILES;
    
    foreach my $file ( @{$tmp_files} ) {
        unlink $file;
    }
    
    close( $self->[ $IDX_LOGHANDLE ] );
    
    if ( $ENV{'SMART_ALERT_ADDRESS'} ) {
        
        # Now email a warning to the Smart Associates alerts address.
        # First, we re-open the log file and read it in so we can dump it into the email.
        
        open DETAIL_LOG, "<" . $self->[ $IDX_LOG_PATH ]
            || die( $! );
        
        my @full_detail_log = <DETAIL_LOG>;
        
        close DETAIL_LOG;
        
        my $body_intro;
        
        if ( defined $job ) {
            
            my $job_id          = $job->key_value;
            my $identifier      = $job->field( SmartAssociates::Database::Item::Job::Base::FLD_IDENTIFIER );
            
            $body_intro         = "A Smart ETL job with Job ID [$job_id]";
            
            if ( $identifier ) {
                $body_intro    .= " ( identifier [$identifier] )";
            }
            
            $body_intro        .= " just failed because:\n$text\n\n";
            
        } elsif ( defined $batch ) {
            
            my $batch_id        = $batch->key_value;
            
            $body_intro         = "A Smart ETL job with Batch ID [$batch_id] just failed because:\n$text\n\n";
            
        }
        
        my $mail = {
            To              => $ENV{'SMART_ALERT_ADDRESS'}
          , From            => $ENV{'SMART_ALERT_FROM'}
          , Sender          => $ENV{'SMART_ALERT_SENDER'}
          , subject         => 'Smart ETL alert'
          , body            => $body_intro . ( '-' x 60 ) . "\n\n" . join( "", @full_detail_log )
          , 'content-type'  => qq(text/plain; charset="utf-8")
          , Smtp            => $ENV{'SMART_ALERT_SMTP_SERVER'}
          , Debug           => 6
          # NP Note will need to use something like Net:SMTP::(TLS|SSL) if we need to auth with more modern smtp relay on anything other than 25
          , Port            => $ENV{'SMART_ALERT_SMTP_PORT'} || 25
        };
        
        if ( $ENV{'SMART_ALERT_SMTP_USER'} ) {
            $mail->{Auth}   =  {
                                    user        => $ENV{'SMART_ALERT_SMTP_USER'}
                                  , pass        => $ENV{'SMART_ALERT_SMTP_PASS'}
                                  , method      => "LOGIN"
                                  , required    => 0
                               };
        }
        
        sendmail( %{$mail} ) or CORE::warn $Mail::Sendmail::error;
    #    print "The sendmail log reports:\n".$Mail::Sendmail::log."\n";
        
    }
    
    my $exit_code;
    
    my $exit_string;
    
    if ( defined $job ) {
        
        # If we're a job, we return a STATUS_COMPLETE exit code.
        # We're using the job control table to manage statuses.
        
        $exit_code = &SmartAssociates::Base::EXIT_CODE_SUCCESSFUL;
        $exit_string = "Job [" . $job->key_value . "] exiting";
        
    } elsif ( defined $batch ) {
        
        # If we're a batch, we return a EXIT_CODE_BATCH_ERROR exit code.
        $exit_code = &SmartAssociates::Base::EXIT_CODE_BATCH_ERROR;
        $exit_string = "Batch [" . $self->globals->BATCH->key_value . "] exiting";
        
    } else {
        
        $exit_code = &SmartAssociates::Base::EXIT_CODE_GROUP_SET_ERROR;
        $exit_string = "Processing group set exiting";
        
    }
    
    $self->info( $exit_string );
    
    exit( $exit_code );
    
}

sub log_level                   { return $_[0]->accessor( $IDX_LOG_LEVEL,                   $_[1] ); }
sub log_level_text              { return $_[0]->accessor( $IDX_LOG_LEVEL_TEXT,              $_[1] ); }
sub log_dir                     { return $_[0]->accessor( $IDX_LOG_DIR,                     $_[1] ); }
sub log_path                    { return $_[0]->accessor( $IDX_LOG_PATH,                    $_[1] ); }

1;
