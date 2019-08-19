package SmartAssociates::ProcessingGroup::ParseNZLog;

use strict;
use warnings;

use File::Path;

use base 'SmartAssociates::ProcessingGroup::Base';

my $IDX_NZ_LOG_PATH                             =  SmartAssociates::ProcessingGroup::Base::FIRST_SUBCLASS_INDEX + 0;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::ProcessingGroup::Base::FIRST_SUBCLASS_INDEX + 1;

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1], $_[2], $_[3], $_[4] );
    
    $self->[ $IDX_NZ_LOG_PATH ]     = $_[5];
    $self->[ $IDX_JOB_ID ]          = $_[6];
    $self->[ $IDX_TARGET_DB ]       = $_[7];
    $self->[ $IDX_TARGET_TABLE ]    = $_[7];
    
    return $self;
    
}

sub prepare {
    
    my $self = shift;
    
}

sub processNZLog {
    
    my $args = shift;
    
    my $nzlog_path = $self->[ $IDX_NZ_LOG_PATH ];
    
    # TODO: we should not have to detokenize at this point. We should only accept
    # a detokenized log path. This decouples us from a TemplateConfig, which is required
    # if we're to be able to run independently ( eg triggered from the command-line )
    # Check whether we ever need to detokenize at this point ...
    
#    $nzlog_path = detokenize( $nzlog_path );
    
    my $nzbad_path = $nzlog_path;
    $nzbad_path =~ s/\.nzlog/\.nzbad/;
    
    my $nzlog;
    
    open( $nzlog, $nzlog_path )
        || logError( "Failed to open NZLOG file: [$nzlog_path]\n" . $! );
    
    my $nzbad;
    
    open( $nzbad, $nzbad_path ); # Don't die if we can't open this
    
    my $dbh = $self->globals->JOB->dbh; # TODO: PORT: check whether JOB always exists. Will it if we're called from the command-line?
    
    my $sth = dbOperationPrepare(
        {
            db      => $dbh
          , sql     => "insert into nzbad\n"
                     . "(\n"
                     . "    job_id\n"
                     . "  , bad_no\n"
                     . "  , input_row\n"
                     . "  , byte_offset\n"
                     . "  , field_no\n"
                     . "  , declaration\n"
                     . "  , diagnostic\n"
                     . "  , text_consumed\n"
                     . "  , last_char_examined\n"
                     . "  , bad_record\n"
                     . "  , target_db\n"
                     . "  , target_table\n"
                     . ") values (\n"
                     . "    ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . "  , ?\n"
                     . ")"
        }
    );
    
    my $bad_records = 0;
    
    while ( my $nzlog_line = <$nzlog> ) {
        
        chomp( $nzlog_line ); # Remove newline character from end of line
        
        # ^  - start of line
        # \d - digit
        # +  - once or more
        # ?  - non-greedy
        # \  - escape some special regex characters
        # () - define a group to be transfered to special variables ( eg $1 $2 )
        # \w - word character ( letter or underscore )
        
        # OK ... now ... sorry about this, but:
        if ( $nzlog_line =~ /(^\d+):\s(\d+)\((\d+)\)\s\[(\d+),\s([\w\s\(\)]+)\]\s([\w\s]+),\s"(.*)"\[(.+?)\]/ ) {
            
            # This line has details of a rejected row
            my $bad_no              = $1;
            my $input_row           = $2;
            my $byte_offset         = $3;
            my $field_no            = $4;
            my $declaration         = $5;
            my $diagnostic          = $6;
            my $text_consumed       = $7;
            my $last_char_examined  = $8;
            
            $bad_records ++;
            
            # Now we read in the corresponding line from the nzbad file
            
            my $nzbad_line = <$nzbad>;
            
            chomp( $nzbad_line );
            
            $self->log->error( "Bad record found:\n"
                    . "{\n"
                    . "              bad #:  $bad_no\n"
                    . "          input row:  $input_row\n"
                    . "        byte offset:  $byte_offset\n"
                    . "            field #:  $field_no\n"
                    . "        declaration:  $declaration\n"
                    . "         diagnostic:  $diagnostic\n"
                    . "      text consumed:  $text_consumed\n"
                    . " last char examined:  $last_char_examined\n"
                    . "        full record:  $nzbad_line\n"
                    . "}\n"
            );
            
            # Execute this direct ... we don't want to trigger a logFail() if this insert fails
            $dbh->execute(
                $sth
              , [
                    $args->{job_id}
                  , $bad_no
                  , $input_row
                  , $byte_offset
                  , $field_no
                  , $declaration
                  , $diagnostic
                  , $text_consumed
                  , $last_char_examined
                  , $nzbad_line
                  , $args->{target_db}
                  , $args->{target_table}
                ]
            );
            
        }
        
    }
    
    $sth->finish();
    
    if ( $nzlog ) {
        close $nzlog;
    }
    
    if ( $nzbad ) {
        close $nzbad;
    }
    
    # Now rename the nzlog and nzbad files. If we try to load the same files again, the client libraries will
    # APPEND to nzlog, but REPLACE nzbad. This will confuse things, including us. We shove the 'main' log timestamp
    # and job id at the start of the filename, which will guarantee that it's unique
    
    for my $log ( ( $nzlog_path, $nzbad_path ) ) {
        
        if ( -e $log ) { # This means "If file $log exists"
            
            my $stripped_log = substr( $log, 0, length( $log ) - 6 );
            my $log_ext      = substr( $log, length( $log ) - 6, 6 );
            
            move( $log, $stripped_log . "." . $LOG_TIMESTAMP . $log_ext );
            
        }
        
    }
    
}

1;
