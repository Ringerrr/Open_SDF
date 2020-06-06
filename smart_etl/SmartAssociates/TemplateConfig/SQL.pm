package SmartAssociates::TemplateConfig::SQL;

use strict;
use warnings;

use JSON;
use File::Find::Rule;                                   # Search files recursively

use base 'SmartAssociates::TemplateConfig::Base';

my $IDX_TARGET_DATABASE_NAME                            =  SmartAssociates::TemplateConfig::Base::FIRST_SUBCLASS_INDEX + 0;

use constant VERSION                                    => '1.6';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    # Detokenize the source & target database
    
    # Recursive parameter substitution in DB names as well ...
    my $source_db_name     = $self->detokenize( $template_config->{SOURCE_DB_NAME} );
    my $source_schema_name = $self->detokenize( $template_config->{SOURCE_SCHEMA_NAME} );
    my $source_table_name  = $self->detokenize( $template_config->{SOURCE_TABLE_NAME} );
    my $target_db_name     = $self->detokenize( $template_config->{TARGET_DB_NAME} );
    my $target_schema_name = $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} );
    my $target_table_name  = $self->detokenize( $template_config->{TARGET_TABLE_NAME} );

    # Recursive parameter substitution in connection name too ...
    my $connection_name    = $self->detokenize( $template_config->{CONNECTION_NAME} );

    $self->target_database_name( $target_db_name );
    
    # Typically SQL templates will have a TARGET_DB_NAME defined ( as we're inserting into a target DB / table ).
    # Sometimes however ( query param & iterator templates ), we're selecting, and only have a SOURCE_DB_NAME defined.
    
    my $active_database = $target_db_name ? $target_db_name : $source_db_name;
    
    $self->target_database( $self->processing_group->target_database( $connection_name, $active_database ) );
    
    if ( $template_config->{BEGIN_TRANSACTION} ) {
        $self->target_database->begin_work;
    }
    
    my $TEMPLATE_TEXT = $template_config->{TEMPLATE_TEXT};
    $TEMPLATE_TEXT = $self->detokenize( $TEMPLATE_TEXT );
    
    $self->execution_preparation;
    
    my $start_ts = $self->log->prettyTimestamp();
    
    $self->log->info( "\n** ** ** ** Executing template SQL: ** ** ** **\n$TEMPLATE_TEXT" );
    
    my $return_info = $self->execute_sql(
        $TEMPLATE_TEXT
    );
    
    my $end_ts = $self->log->prettyTimestamp();

    my $custom_logs = $return_info->{custom_logs};             # Any custom logs that have already been returned

    $custom_logs = $self->collect_custom_logs( $custom_logs ); # Any other, as-yet unhanlded custom logs

    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , ( $target_db_name     ? $target_db_name     : $source_db_name )
      , ( $target_schema_name ? $target_schema_name : $source_schema_name )
      , ( $target_table_name  ? $target_table_name  : $source_table_name )
      , $start_ts
      , $end_ts
      , $return_info->{record_count}
      , $return_info->{error}
      , ( $return_info->{template_text} ? $return_info->{template_text} : $TEMPLATE_TEXT )
      , to_json( $self->perf_stats, { pretty => 1 } )
      , $template_config->{NOTES}
      , $custom_logs
    );
    
    $self->globals->LAST_STEP_RECORD_AFFECTED( $return_info->{record_count} );
    
    if ( $return_info->{error} ) {
        $self->log->fatal( $return_info->{error} ); # The logger takes care of rolling back target databases
    }
    
    # If we're gotten down here, we've successfully completed the current step ( template )
    # Now we check if we've been instructed to commit at this point. If so, we loop through
    # all open target database handles, commit them, disconnect them, and remove them from
    # our hash of open target handles
    
    if ( $template_config->{COMMIT_STEP} ) {
        $self->log->info( "Committing all work ..." );
        $self->processing_group->commit_target_databases();
    }
    
}

sub execute_sql {
    
    my ( $self, $TEMPLATE_TEXT ) = @_;
    
    my ( $sth, $record_count, $error );
    
    my $template_config = $self->template_record;
    
    eval {

        $self->log->info( 'Preparing SQL for execution' );
        $self->perf_stat_start( 'Template SQL preparing in database engine' );

        $sth = $self->target_database->prepare( $TEMPLATE_TEXT )
            || die( $self->target_database->errstr );

        $self->perf_stat_stop( 'Template SQL preparing in database engine' );
        $self->log->info( 'Statement handle successfully prepared from SQL' );

        if ( ! $template_config->{UNPACKED_JOB_ARGS}->{simulate} ) {
            
            $self->perf_stat_start( 'Template SQL execution in database engine' );

           # $record_count = $sth->execute()
           #     || die( $sth->errstr );
            
            $record_count = $sth->execute();
            my $sth_errstr = $sth->errstr;

            if ( $sth_errstr ) {
                $self->log->info( "Caught error message:\n$sth_errstr" );
                # See if this was an error flagged to downgrade to a warning.
                my $error_strings_to_downgrade = $self->target_database->error_strings_to_downgrade();
                my $success = 0;
                foreach my $err_str ( @{$error_strings_to_downgrade} ) {
                    $self->log->info( "Testing pattern to downgrade for this DB: [$err_str]" );
                    if ( $sth_errstr =~ /$err_str/gi ) {
                        $self->log->warn( "Matched an 'error' message string: [$err_str] that we've been instructed to downgrade to a warning. Full error from database:\n$sth_errstr" );
                        $success = 1;
                        last;
                    } else {
                        $self->log->info( "Pattern [$err_str] did NOT match ..." );
                    }
                }
                if ( ! $success ) {
                    die( $sth_errstr );
                }
            }
            
            $self->perf_stat_stop( 'Template SQL execution in database engine' );
            
        }
        
    };
    
    $error = $@;
    
    if ( ! $error ) {

        my $return_value;

        eval {
            $return_value = $self->handle_executed_sth( $sth ); # subclasses do some extra stuff in here ...
        };

        $error = $@;

        if ( $record_count == -1 && $return_value ) {   # an insert, and the subclass processed something and maybe counted things
            $record_count = $return_value;
        } elsif ( $record_count eq &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED && $return_value ) {  # mysql returns 0 when we force mysql_use_result=1
            $record_count = $return_value;
        }

        $self->execution_completion();

    }

    my $execution_stats = $self->target_database->capture_execution_info( $sth );

    foreach my $stat_key ( keys %{$execution_stats} ) {
        $self->perf_stat( $stat_key , $execution_stats->{ $stat_key } );
    }

    if ( $sth ) {
        $sth->finish();
    }

    return {
        record_count  => $record_count
      , error         => $error
    };
    
}

sub execution_preparation {
    
    my $self = shift;
    
    return;
    
}

sub execution_completion {

    my $self = shift;

    return;

}

sub handle_executed_sth {
    
    my $self = shift;
    
    return;
    
}

sub target_database_name    { return $_[0]->accessor( $IDX_TARGET_DATABASE_NAME,        $_[1] ); }

1;
