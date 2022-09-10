package SmartAssociates::TemplateConfig::SQL::DatabaseMethod;

use strict;
use warnings;

use JSON;

# This class redirects logic from the TemplateConfig class to a database-specific class.
# We call the template name's method in the database class, and also pass ourself in.
# The idea here is that we implement all database-specific template code inside the database
# class itself, and don't end up polluting the TemplateConfig namespace with lots of small
# database-specific classes. It also prevents us from having to copy boilerplate code each
# time we have a database-specific task

use base 'SmartAssociates::TemplateConfig::SQL';

use constant VERSION                            => '1.1';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;
    
    $self->log->info( "\n--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Unpack job args from JSON string
    if ( $template_config->{JOB_ARGS} ) {
        $template_config->{UNPACKED_JOB_ARGS} = decode_json( $template_config->{JOB_ARGS} );
    } else {
        $template_config->{UNPACKED_JOB_ARGS} = {};
    }
    
    # Recursive parameter substitution in DB names as well ...
    my $source_db_name     = $self->detokenize( $template_config->{SOURCE_DB_NAME} );
    my $source_schema_name = $self->detokenize( $template_config->{SOURCE_SCHEMA_NAME} );
    my $source_table_name  = $self->detokenize( $template_config->{SOURCE_TABLE_NAME} );
    my $target_db_name     = $self->detokenize( $template_config->{TARGET_DB_NAME} );
    my $target_schema_name = $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} );
    my $target_table_name  = $self->detokenize( $template_config->{TARGET_TABLE_NAME} );

    # Recursive parameter substitution in connection_name too ...
    my $connection_name    = $self->detokenize( $template_config->{CONNECTION_NAME} );

    $self->target_database_name( $target_db_name );
    
    # Typically SQL templates will have a TARGET_DB_NAME defined ( as we're inserting into a target DB / table ).
    # Sometimes however ( query param & iterator templates ), we're selecting, and only have a SOURCE_DB_NAME defined.
    
    my $active_database = $target_db_name ? $target_db_name : $source_db_name;
    
    my $target_connection = $self->processing_group->target_database( $connection_name, $active_database );
    $self->target_database( $target_connection );
    
    # Now that we've got our connection, redirect logic into the connection class
    my $return;
    
    my $method = $self->resolve_parameter( '#P_ZZ_DATABASE_CLASS_METHOD#' ) || $template_config->{TEMPLATE_NAME};
    
    if ( $target_connection->can( $method ) ) {
        $return = $self->target_database->$method( $self );
    } else {
        die( "Target connection doesn't implement method " . $template_config->{TEMPLATE_NAME} . "!" );
    }
    
    my $end_ts = $self->log->prettyTimestamp();
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $connection_name
      , ( $source_schema_name || $target_schema_name )
      , ( $source_table_name || $target_schema_name )
      , $start_ts
      , $end_ts
      , $return->{record_count}
      , $return->{error}
      , $return->{template_text}
      , to_json( $self->perf_stats, { pretty => 1 } )
      , $template_config->{NOTES}
      , $template_config->{custom_logs}
    );
    
    if ( $return->{error} ) {
        $self->log->fatal( $return->{error} );
    }
    
}

1;
