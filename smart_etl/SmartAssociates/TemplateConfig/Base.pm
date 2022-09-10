package SmartAssociates::TemplateConfig::Base;

use strict;
use warnings;

use Time::HiRes;
use File::Temp qw/ tempfile /;
use JSON;

use base 'SmartAssociates::Base';

my $IDX_PROCESSING_GROUP_OBJECT                 =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  0;
my $IDX_SEQUENCE_ORDER                          =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  1;
my $IDX_TEMPLATE_RECORD                         =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  2;
my $IDX_PARAMETERS                              =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  3;

my $IDX_TARGET_DATABASE                         =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  4;
my $IDX_CHILD_TEMPLATE_CONFIGS                  =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  5;
my $IDX_IPC_TASK                                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  6;
my $IDX_PERF_STATS_HASH                         =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  7;
my $IDX_PERF_STATS_MARKERS                      =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  8;
my $IDX_RECURSION_COUNTER                       =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  9;
my $IDX_PROCESSING_GROUP_ARGS                   =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 10;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 11;

use constant    ENV_HIGH_DATE_TIME              => '2999-12-31 23:59:59.999999';
use constant    ENV_HIGH_DATE                   => '2999-12-31';

use constant    VERSION                         => '2.0';

# This is the base class of the 'main brains' of the ETL framework. This class encapsulates
# a combination of a TEMPLATE record and associated LOAD_CONFIG record

sub new {
    
    # TODO: pass SEQUENCE_ORDER from command-line
    
    my $self   = $_[0]->SUPER::new(           $_[1] );
    
    $self->[ $IDX_PROCESSING_GROUP_OBJECT ] = $_[2];
    $self->[ $IDX_SEQUENCE_ORDER ]          = $_[3];
    $self->[ $IDX_IPC_TASK ]                = $_[4];
    
    # NOTE: if you add more args to the constructor, you MUST alter subclasses accordingly ...
    # ( there are currently no subclasses that override the constructor )
    
    $self->[ $IDX_CHILD_TEMPLATE_CONFIGS ] = [];
    $self->[ $IDX_PERF_STATS_HASH ]        = {};
    $self->[ $IDX_PERF_STATS_MARKERS ]     = {};
    $self->[ $IDX_RECURSION_COUNTER ]      = 0;
    $self->[ $IDX_PROCESSING_GROUP_ARGS ]  = $self->globals->PROCESSING_GROUP_ARGS();

    return $self;
    
}

sub add_child_template_config {
    
    my ( $self, $child_template_config ) = @_;
    
    push @ {$self->[ $IDX_CHILD_TEMPLATE_CONFIGS ] }, $child_template_config;
    
}

sub prepare {
    
    my $self = shift;
    
    if ( ! $self->[ $IDX_TEMPLATE_RECORD ] ) {
        $self->getMetadata();
    }
    
}

sub collect_custom_logs {

    my ( $self , $custom_logs ) = @_;

    # This method collects any custom logs in our temp job log directory,
    # and *merges* them into any custom logs passed into us. This allows for
    # custom parsing / handling of logs, as well as our generic handling here.

    my @fileList;
    push @fileList, File::Find::Rule->file()
                                    ->name( "*" )
                                    ->in( $self->globals->JOB->job_log_dir() );

    if ( @fileList ) {
        $self->log->info( "Found custom logs:\n" . to_json( \@fileList , { pretty => 1 } ) );
    }

    my $log_sequences;

    foreach my $file_path ( @fileList ) {

        no warnings 'uninitialized';

        local $/; # Slurp in entire files at once

        open( CUSTOM_LOG_HANDLE , "<$file_path" )
            || die( "Failed to open custom log file ( $file_path ):\n" . $! );

        my $this_custom_log = <CUSTOM_LOG_HANDLE>;

        close CUSTOM_LOG_HANDLE;

        my $suffix = '';

        if ( $file_path =~ /.*\.(.*)/ ) {
            $suffix = $1;
            $self->log->info( "Found custom log suffix [$suffix]" );
        }

        my $this_log_name;

        # We may already have a custom log in $custom_logs with the current sequence, so keep incrementing our log sequence
        # until we find a unique name ...
        do {
            $log_sequences->{ $suffix } ++;
            $this_log_name = ( $suffix ne '' ? $suffix . " " : "" ) . $log_sequences->{ $suffix };
        } while ( exists $custom_logs->{ $this_log_name } );

        $self->log->info( "Generated custom log name: [$this_log_name] for original custom log: [$file_path]" );

        $custom_logs->{ $this_log_name } = $this_custom_log;

        unlink $file_path; # If we don't delete files now, the *next* step will read them and add them too

    }

    return $custom_logs;

}

sub getMetadata {
    
    my $self = shift;
    
    my $dbh = $self->[ $IDX_PROCESSING_GROUP_OBJECT ]->dbh;
    
    my $sth = $dbh->prepare(
        "select\n"
      . "    CONFIG.SEQUENCE_ORDER\n"
      . "  , CONFIG.PARENT_SEQUENCE_ORDER\n"
      . "  , CONFIG.CONNECTION_NAME\n"
      . "  , CONFIG.TARGET_DB_NAME\n"
      . "  , CONFIG.TARGET_SCHEMA_NAME\n"
      . "  , CONFIG.TARGET_TABLE_NAME\n"
      . "  , CONFIG.SOURCE_DB_NAME\n"
      . "  , CONFIG.SOURCE_SCHEMA_NAME\n"
      . "  , CONFIG.SOURCE_TABLE_NAME\n"
      . "  , CONFIG.BEGIN_TRANSACTION\n"
      . "  , CONFIG.COMMIT_STEP\n"
      . "  , CONFIG.ON_ERROR_CONTINUE\n"
      . "  , CONFIG.PROCESSING_GROUP_NAME\n"
      . "  , CONFIG.NOTES\n"
      . "  , TEMPLATE.TEMPLATE_NAME\n"
      . "  , TEMPLATE.TEMPLATE_DESC\n"
      . "  , TEMPLATE.TEMPLATE_TEXT\n"
      . "  , TEMPLATE.CLASS\n"
      . "from\n"
      . "            CONFIG         CONFIG\n"
      . "inner join  TEMPLATE       TEMPLATE\n"
      . "    on\n"
      . "            CONFIG.TEMPLATE_NAME            = TEMPLATE.TEMPLATE_NAME\n"
      . "where\n"
      . "    CONFIG.PROCESSING_GROUP_NAME            = ?\n"
      . "and CONFIG.SEQUENCE_ORDER                   = ?\n"
    );
    
    $dbh->execute(
        $sth
      , [
            $self->[ $IDX_PROCESSING_GROUP_OBJECT ]->name
          , $self->[ $IDX_SEQUENCE_ORDER ]
        ]
    );
    
    $self->[ $IDX_TEMPLATE_RECORD ] = $sth->fetchrow_hashref;
    
    $sth->finish();
    
    $self->[ $IDX_TEMPLATE_RECORD ]->{JOB_ARGS} = $self->globals->JOB->field(
        &SmartAssociates::Database::Item::Job::Base::FLD_JOB_ARGS
    );

    # Unpack job args from JSON string
    if ( $self->[ $IDX_TEMPLATE_RECORD ]->{JOB_ARGS} ) {
        $self->[ $IDX_TEMPLATE_RECORD ]->{UNPACKED_JOB_ARGS} = decode_json( $self->[ $IDX_TEMPLATE_RECORD ]->{JOB_ARGS} );
    } else {
        $self->[ $IDX_TEMPLATE_RECORD ]->{UNPACKED_JOB_ARGS} = {};
    }

    # First get a list of populated PARAM_VALUEs
    $sth = $dbh->prepare(
        "select\n"
      . "    PARAM.PARAM_NAME\n"
      . "  , PARAM.PARAM_DESC\n"
      . "  , PARAM.PARAM_DEFAULT\n"
      . "  , PARAM_VALUE.PARAM_VALUE\n"
      . "from\n"
      . "            PARAM\n"
      . "inner join\n"
      . "            PARAM_VALUE\n"
      . "on\n"
      . "            PARAM.PARAM_NAME = PARAM_VALUE.PARAM_NAME\n"
      . "where\n"
      . "            PARAM_VALUE.PROCESSING_GROUP_NAME = ?\n"
      . "  and       PARAM.TEMPLATE_NAME               = ?\n"
      . "  and       PARAM_VALUE.SEQUENCE_ORDER        = ?"
    );
    
    $dbh->execute(
        $sth
      , [
            $self->[ $IDX_PROCESSING_GROUP_OBJECT ]->name
          , $self->[ $IDX_TEMPLATE_RECORD ]->{TEMPLATE_NAME}
          , $self->[ $IDX_SEQUENCE_ORDER ]
        ]
    );
    
    $self->[ $IDX_PARAMETERS ] = $sth->fetchall_hashref( 'PARAM_NAME' );
    
    # ... and then merge in defaults ( including where there are NO PARAM_VALUE records )
    $sth = $dbh->prepare(
        "select\n"
      . "    PARAM.PARAM_NAME\n"
      . "  , PARAM.PARAM_DESC\n"
      . "  , PARAM.PARAM_DEFAULT\n"
      . "from\n"
      . "    PARAM\n"
      . "where\n"
      . "    PARAM.TEMPLATE_NAME = ?"
    );
    
    $dbh->execute(
        $sth
      , [
            $self->[ $IDX_TEMPLATE_RECORD ]->{TEMPLATE_NAME}
        ]
    );
    
    my $defaults = $sth->fetchall_hashref( 'PARAM_NAME' );
    
    foreach my $param ( keys %{$defaults} ) {
        $self->[ $IDX_PARAMETERS ]->{ $param }->{PARAM_DEFAULT} = $defaults->{ $param }->{PARAM_DEFAULT};
    }
    
    $sth->finish();
    
}

sub execute {
    
    my $self = shift;
    
    $self->log->fatal( "Can't call execute() on SmartAssociates::TemplateConfig::Base. Sub-classes MUST implement this method" );
    
}

sub detokenize {
    
    my ( $self, $template_sql ) = @_;

    $self->[ $IDX_RECURSION_COUNTER ] ++;

    my $parameter_recursion_limit = $self->globals->PARAMETER_RECURSION_LIMIT;

    my @substitution_parameters;
    {
        no warnings 'uninitialized';
        @substitution_parameters = $template_sql =~ /#[a-zA-Z0-9_\.:=,]+#/g;
    }

    while ( ( $self->[ $IDX_RECURSION_COUNTER ] < $parameter_recursion_limit ) && @substitution_parameters ) {
        
        @substitution_parameters = $template_sql =~ /#[a-zA-Z0-9_\.:=,]+#/g;
        
        foreach my $parameter ( @substitution_parameters ) {
            
            my $value = $self->resolve_parameter( $parameter );
            
            if ( defined $value ) {
                $self->log->info( "Substituting value [$value] for parameter [$parameter]" );
            } else {
                $self->log->info( "Substituting EMPTY STRING for parameter [$parameter]" );
            }
            
            {
                no warnings 'uninitialized';
                $template_sql =~ s/$parameter/$value/g;
            }
            
        }
        
    }

    if ( $self->[ $IDX_RECURSION_COUNTER ] == $parameter_recursion_limit ) {
        $self->log->info( "Hit parameter recursion limit. One more level would be fatal ..." );
    } elsif ( $self->[ $IDX_RECURSION_COUNTER ] > $parameter_recursion_limit ) {
        $self->log->fatal( "Exceeded recursion limit!" );
    }

    $self->[ $IDX_RECURSION_COUNTER ] --;

    return $template_sql;
    
}

sub resolve_parameter {
    
    my ( $self, $parameter ) = @_;

    $self->[ $IDX_RECURSION_COUNTER ] ++;

    my $parameter_recursion_limit = $self->globals->PARAMETER_RECURSION_LIMIT;
    
    my $is_token = 1;
    
    my $value;
    
    while ( $self->[ $IDX_RECURSION_COUNTER ] < $parameter_recursion_limit && $is_token ) {

        my @substitution_parameters = $parameter =~ /#["'a-zA-Z0-9_\.:=,]+#/g;
        
        # One parameter can expand into multiple, and in this case, we
        # use detokenize() to deal with them
        
        # Also we call detokenize if there is text outside the tokens ( ie outside the hash # )
        if ( @substitution_parameters > 1
           || $substitution_parameters[0] ne $parameter
#          || $parameter =~/^#([\w]*)#.+/ 
        ) {
            
            $value = $self->detokenize( $parameter );
            
        } elsif (      $parameter =~ /^#CONFIG/ ) {
            
            $value = $self->resolveConfigParameter( $parameter );
            
        } elsif ( $parameter =~ /^#ENV/ ) {
            
            $value = $self->resolveEnvironmentParameter( $parameter );
            
        } elsif ( $parameter =~ /^#P_/ ) {
            
            $value = $self->resolveUserParameter( $parameter );
            
        } elsif ( $parameter =~ /^#COMPLEX/ ) {
            
            $value = $self->resolveComplexParameter( $parameter );
            
        } elsif ( $parameter =~ /^#Q_([\w]*)#$/ ) {
            
            # Q_ parameters are populated by 'SQL_TO_PARAMS' templates ...
            
            my $column_name = $1;
            
            my $query_parameters = $self->globals->Q_PARAMS;
            
            if ( exists $query_parameters->{ $column_name } && defined $query_parameters->{ $column_name } ) {
                $value = $query_parameters->{ $column_name };
            } else {
                $self->log->info( "resolve_parameter didn't find any value for the user-query parameter [$parameter]. Returning undef ( which will equate to EMPTY STRING )." );
                $value = '';
            }
            
        } elsif ( $parameter =~ /^#I_([\w]*)\.([\w]*)#$/ ) {
            
            # I_ parameters are references to fields in an iterator
            
            my $iterator_name = $1;
            my $column_name   = $2;
            
            $value = $self->globals->ITERATOR( $iterator_name )->get_item( $column_name );
            
        } elsif ( $parameter =~ /^#J_([\w]*)#$/ ) {
            
            my $job_parameter_key = $1;
            
            my $template_config = $self->template_record;
            
            if (   exists $template_config->{UNPACKED_JOB_ARGS}->{ $job_parameter_key } ) {    # these are job-specific args, packed into job_ctl.job_args
                
                $self->log->info( "Using JSON-encoded value from JOB_CTL.job_args [" . $template_config->{UNPACKED_JOB_ARGS}->{ $job_parameter_key } . "] for parameter: [$job_parameter_key]" );
                $value = $template_config->{UNPACKED_JOB_ARGS}->{ $job_parameter_key };
                
            } else {
                
                $self->log->warn( "Parameter [$job_parameter_key] requested, but doesn't exist in job_ctl record!" );
                $value = '';
                
            }
            
        } else {
            
            # The 1st time through, we shouldn't get here. If we have a recursive
            # token, however, and our anchored regexes above ( ^token$ ) don't match,
            # then we may have a more complex string with a token somewhere inside it.
            # In that case, we fall back to the detokenize() method.
            
            $value = $self->detokenize( $parameter );
            
        }
        
        if ( ! defined $value ) {
            $is_token  = 0;
        } elsif ( $value =~ /#.+#/ ) {
            $is_token  = 1;
            $parameter = $value;
        } else {
            $is_token  = 0;
        }
        
    }

    if ( $self->[ $IDX_RECURSION_COUNTER ] == $parameter_recursion_limit ) {
        $self->log->info( "Hit parameter recursion limit. One more level would be fatal ..." );
    } elsif ( $self->[ $IDX_RECURSION_COUNTER ] > $parameter_recursion_limit ) {
        $self->log->fatal( "Exceeded recursion limit!" );
    }

    $self->[ $IDX_RECURSION_COUNTER ] --;

    return $value;
    
}

sub resolveConfigParameter {
    
    my ( $self, $parameter ) = @_;
    
    my $template_config = $self->template_record;
    my $value;
    
    # ( most ) CONFIG parameters are built from LOAD_CONFIG values
    
    $parameter =~ s/^#CONFIG_//;
    $parameter =~ s/#$//;
    
    if ( exists $template_config->{ $parameter } ) {
        
        $value = $template_config->{ $parameter }
        
    } else {
        
        $self->log->warn( "Config parameter [$parameter] does not exist in our configuration hash" );
        
    }
    
    return $value;
        
}

sub resolveComplexParameter {

    my ( $self, $parameter ) = @_;

    my $parameters = $self->parameters;

    my ( $method_name , $return_value , $modifier );

    if ( $parameter =~ /^#([\w_]*):*([\w_=,]*)#$/ ) {
        ( $method_name , $modifier ) = ( $1 , $2 );
    } else {
        $self->log->fatal( "Failed to parse method name from complex parameter: [$parameter]" );
    }

    my $resolver_found;

    foreach my $resolver ( @{$self->globals->RESOLVERS()} ) {
        if ( $resolver->can( $method_name ) ) {
            $self->log->info( "Complex token [$parameter] logic found in [" . $resolver->name() . "] resolver class" );
            $return_value = $resolver->$method_name( $modifier , $self , $parameters );
            $resolver_found = 1;
            last;
        }
    }

    if ( ! $resolver_found ) {
        $self->log->warn( "No resolver had logic for complex token [$parameter]" );
    }

    return $return_value;

}

sub resolveUserParameter {

    my ( $self, $parameter ) = @_;

    my $parameters          = $self->parameters;
    my $template_config     = $self->template_record;

    my $value;

    # We start out with parameters that can be overridden with command-line options ...
    if (   $parameter eq '#P_MAX_ERRORS#' ) {                                   # the max allowed errors per external table load
        my $max_errors = $self->globals->MAX_ERRORS;
        if ( $max_errors ) {
            $self->log->debug( "Max errors value of [$max_errors] was passed in from command-line. Using it ..." );
            $value = $max_errors;
            return $value;                  # this overrides user metadata, so we return straight away
        }
    }

    my $param_name;
    if ( $parameter =~ /#P_(.*)#/ ) {
        $param_name = $1;
    }
    if (   exists $parameters->{ $parameter } && exists $parameters->{ $parameter }->{PARAM_VALUE} ) {
        $self->log->info( "Using param_value [" . $parameters->{ $parameter }->{PARAM_VALUE} . "] for parameter: [$parameter]" );
        $value = $parameters->{ $parameter }->{PARAM_VALUE};
    } elsif (   exists $template_config->{UNPACKED_JOB_ARGS}->{ $parameter } ) {
        # these are job-specific args, packed into job_ctl.job_args
        $self->log->info( "Using JSON-encoded value from JOB_CTL.job_args [" . $template_config->{UNPACKED_JOB_ARGS}->{ $parameter } . "] for parameter: [$parameter]" );
        $value = $template_config->{UNPACKED_JOB_ARGS}->{ $parameter };
    } elsif (   exists $self->[ $IDX_PROCESSING_GROUP_ARGS ]->{ $parameter } ) {
        # these come from JOB_ARGS_JSON in the PROCESSING_GROUP record
        $self->log->info( "Using JSON-encoded value from PROCESSING_GROUP.JOB_ARGS_JSON [" . $self->[ $IDX_PROCESSING_GROUP_ARGS ]->{ $parameter } . "] for parameter: [$parameter]" );
        $value = $self->[ $IDX_PROCESSING_GROUP_ARGS ]->{ $parameter };
    } elsif (   exists $parameters->{ $parameter }->{PARAM_DEFAULT} ) {
        no warnings 'uninitialized';
        $self->log->info( "Using default value [" . $parameters->{ $parameter }->{PARAM_DEFAULT} . "] for parameter: [$parameter]" );
        $value = $parameters->{ $parameter }->{PARAM_DEFAULT};
    } else {
        my $q_param = $self->resolve_parameter( '#Q_' . $param_name . '#' );
        if ( $q_param ne '' ) {
            $self->log->info( "Using #Q_" . $param_name . "# value [$q_param] for parameter: [$parameter]" );
            $value = $q_param;
        } else {
            if (
                substr( $parameter , 0 , 5 ) ne '#P_ZZ'
                    && $parameter ne '#P_ITERATOR#'
                    && $parameter ne '#P_LOOP#'
            ) {
                $self->log->warn( "resolve_parameter() was passed an unknown parameter: [$parameter]" );
            }
        }
    }

    return $value;

}

sub resolveEnvironmentParameter {

    my ( $self, $parameter ) = @_;

    my $template_config = $self->template_record;

    my $value;

    # Allow overriding with values encoded in the job table ...
    #  ... the #ENV_FIFO_PATH# for example can be encoded in by the batch

    if ( exists $template_config->{UNPACKED_JOB_ARGS}->{$parameter} ) {

        $value = $template_config->{UNPACKED_JOB_ARGS}->{$parameter};

    } else {

        if (        $parameter eq '#ENV_JOB_ID#' ) {

            $value = $self->globals->JOB->key_value;

        } elsif (   $parameter eq '#ENV_TIMESTAMP#' ) {

            my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);

            $value = sprintf( "%04d-%02d-%02d %02d:%02d:%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );

        } elsif (   $parameter eq '#ENV_LOG_DIR#' ) {                                   # the log directory

            $value = $self->log->log_dir;

        } elsif (   $parameter eq '#ENV_JOB_LOG_DIR#' ) {

            $value = $self->log->log_dir . "/" . $self->globals->JOB->key_value . "/";

        } elsif (   $parameter eq '#ENV_HIGH_DATE#' ) {                                   # used to indicate a record is 'open'

            $value = ENV_HIGH_DATE;

        } elsif ( $parameter eq '#ENV_EXTRACT_SEQUENCE#' ) {

            my $extraction_state = $self->processing_group->extraction_state;

            if ( ! $extraction_state ) {
                $self->log->fatal( "We didn't get an extraction state object! Something is horribly wrong" );
            }

            $value = $extraction_state->key_value;

        } elsif ( $parameter eq '#ENV_FIFO_FILE#' ) {

            my $full_path = $self->resolve_parameter( '#ENV_FIFO_PATH#' ); # so this would just be recursive ... wtf ???

            if ( $full_path =~ /.*\/(.*)/ ) {
                $value = $1;
            } else {
                $self->log->fatal( "Failed to parse FIFO filename out of FIFO path: [$full_path]" );
            }

        } elsif ( $parameter eq '#ENV_HARVEST_PATH#' ) {

            my $root_dir = $ENV{HARVEST_ROOT_DIR} || $self->log->fatal( "Environment variable HARVEST_ROOT_DIR missing!" );

            my $dir_separator = $self->globals->DIR_SEPARATOR;

            $value = $root_dir
                . $dir_separator . 'processing' . $dir_separator
                . $self->globals->JOB->field( &SmartAssociates::Database::Item::Job::Base::FLD_IDENTIFIER );

        } elsif ( $parameter eq '#ENV_HARVEST_FILENAME#' ) {

            $value = $self->globals->JOB->field( &SmartAssociates::Database::Item::Job::Base::FLD_IDENTIFIER );

        } elsif ( $parameter eq '#ENV_RECORDS_AFFECTED#' ) {

            $value = $self->globals->LAST_STEP_RECORD_AFFECTED;

        } elsif ( $parameter eq '#ENV_CONTROL_DB_NAME#' ) {

            $value = $self->globals->CONTROL_DB_NAME;

        } elsif ( $parameter eq '#ENV_LOG_DB_NAME#' ) {

            $value = $self->globals->LOG_DB_NAME;

        } elsif ( $parameter eq '#ENV_EXTRACT_DATE#' ) {

            $value = $self->globals->EXTRACT_DATE || $self->log->date;

        } else {

            my ( $stripped_param , $value );

            if ( $parameter =~ /#.*#/ ) {
                $stripped_param = $1;
                if ( exists $ENV{$parameter} ) {
                    $value = $ENV{$parameter};
                    $self->log->debug( "Resolving [$parameter] from environment variable" );
                }
            }

            $self->log->warn( "resolve_parameter() was passed an unknown environment parameter: [$parameter]" );

        }

    }

    return $value;

}

sub complete {
    
    my $self = shift;
    
    # Here we execute any child templates. If we have any, we're EITHER
    # an iterator ( we'll have a #P_ITERATOR# parameter defined ) OR
    # a LOOP_FROM template ( we'll have a #P_LOOP# parameter defined ).

    # 1)
    # Note that loops are implemented using an iterator, but we shove _SDF_LOOP_ at
    # the start of the name ( to avoid potential name collisions with other iterators
    # users might create ).

    # 2)
    # A quick note about our naming convention here ...
    # When DEFINING an iterator or a loop, we DON'T have the string 'NAME' in the
    # variable. When *accessing* the iterator or loop, we DO have the string 'NAME'
    # in the variable. This allows us to differentiate between the creation of
    # iterators and loops, and simply accessing data inside the underlying iterator.
    # If we didn't use this different naming convention, the logic below would fail,
    # as any step that makes use of an iterator would be detected as the *start* of
    # an iterator ( ie we'd loop forever, below ).

    my $iterator_name = $self->resolve_parameter( '#P_ITERATOR#' );
    
    if ( ! $iterator_name ) {
        $iterator_name = $self->resolve_parameter( '#P_LOOP#' );
        if ( $iterator_name ) {
            $iterator_name = '_SDF_LOOP_' . $iterator_name;
        } else {
            # No iterator and no loop ... exit ...
            return;
        }
    }
    
    my $iterator      = $self->globals->ITERATOR( $iterator_name );
    
    if ( ! $iterator->count_items ) {
        return; # No rows to iterate over
    }

    # A note on order ...
    # If there are a number of templates in an iterator, most people would expect the OUTER loop to be the iterator
    # This allows incremental processing to flow through each template in a natural way
    
    do {
        ITERATOR_LOOP:
        {
            foreach my $template ( @{$self->[ $IDX_CHILD_TEMPLATE_CONFIGS ]} ) {
                $template->prepare();
                # we store this so Log.pm can easily get at it to decide whether to downgrade fatals if ON_ERROR_CONTINUE is set
                # note that the main loop ( ProcessingGroup::Base ) also set this
                $self->globals->CURRENT_TEMPLATE_CONFIG( $template );
                eval {
                    $template->execute();
                };
                my $err = $@;
                if ( $err ) {
                    $self->log->fatal( "Caught an error while executing a child step:\n\n$err." );
                }
                $template->complete();
            }
        }
    } while $iterator->iterate();
    
}

sub perf_stat {
    
    my ( $self, $stat_type, $stat_value ) = @_;
    
    # This is a getter / setter for stat types
    
    if ( $stat_type && $stat_value ) {
        $self->[ $IDX_PERF_STATS_HASH ]->{ $stat_type } += $stat_value;
    }
    
    return $self->[ $IDX_PERF_STATS_HASH ]->{ $stat_type } || 0;
    
}

sub perf_stat_start {
    
    my ( $self, $stat_type ) = @_;
    
    # This starts a timer for a stat type
    
    $self->[ $IDX_PERF_STATS_MARKERS ]->{ $stat_type } = Time::HiRes::gettimeofday;
    
}

sub perf_stat_stop {
    
    my ( $self, $stat_type ) = @_;
    
    # This ends a timer for a stat type, adds the elapsed time to the stat, and returns the *instance* time ( not aggregate )
    my $this_stat_time = Time::HiRes::gettimeofday - $self->[ $IDX_PERF_STATS_MARKERS ]->{ $stat_type };
    
    $self->[ $IDX_PERF_STATS_HASH ]->{ $stat_type } += $this_stat_time;
    
    return $this_stat_time;
    
}

sub template_record             { return $_[0]->accessor( $IDX_TEMPLATE_RECORD,         $_[1] ); }
sub processing_group            { return $_[0]->accessor( $IDX_PROCESSING_GROUP_OBJECT, $_[1] ); }
sub parameters                  { return $_[0]->accessor( $IDX_PARAMETERS,              $_[1] ); }
sub target_database             { return $_[0]->accessor( $IDX_TARGET_DATABASE,         $_[1] ); }
sub perf_stats                  { return $_[0]->accessor( $IDX_PERF_STATS_HASH,         $_[1] ); }

sub on_error_continue           { return $_[0]->[ $IDX_TEMPLATE_RECORD ]->{ON_ERROR_CONTINUE};   } # read-only

1;
