package SmartAssociates::Database::Connection::BigQuery;

use strict;
use warnings;

use Google::BigQuery;
use JSON;
use String::CamelCase qw(camelize decamelize wordsplit);
use Text::CSV;
use File::Basename;

use base 'SmartAssociates::Database::Connection::Base';

my $IDX_REST_CLIENT                             =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_CLIENT_EMAIL                            =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_PRIVATE_KEY_FILE                        =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_PROJECT_ID                              =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 3;

use constant FIRST_SUBCLASS_INDEX                                                                                   => 5;

use constant DB_TYPE                            => 'BigQuery';

sub build_connection_string {
    
    my ( $self, $credentials, $database ) = @_;
    
    # As BigQuery doesn't use *JUST* a traditional DBI-class driver,
    # we instead JSON-encode the credentials and return them
    # as the connection string, which then gets passed to
    # connection() below, and passed into the Google::BigQuery
    # constructor. We also use Google's ODBC driver, so we construct
    # the connection string for it in connect(), below
    
    my $connection_string = encode_json( $credentials );
    
    return $connection_string;
    
}

sub connect_do {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    # Store these in properly named attributes so we're not translating from our regular auth hash key names
    
    $self->[ $IDX_CLIENT_EMAIL ]     = $auth_hash->{Username};
    $self->[ $IDX_PRIVATE_KEY_FILE ] = $auth_hash->{Password};
    $self->[ $IDX_PROJECT_ID ]       = $auth_hash->{Database};
    
    $self->[ $IDX_REST_CLIENT ] = Google::BigQuery::create(
        client_email        => $self->[ $IDX_CLIENT_EMAIL ]
      , private_key_file    => $self->[ $IDX_PRIVATE_KEY_FILE ]
      , project_id          => $self->[ $IDX_PROJECT_ID ]
      , debug               => 1
    ) || $self->log->fatal( "Could not connect to " . $self->DB_TYPE . " database via REST API ( Google::BigQuery )\n" . DBI->errstr );
    
    my $connection_string;
    
    {
        no warnings "uninitialized";
        $connection_string =
              "dbi:ODBC:"
            . "DRIVER="               . $auth_hash->{ODBC_Driver}
            . ";OAuthMechanism=0"
            . ";Email="               . $auth_hash->{Username}
            . ";KeyFilePath="         . $auth_hash->{Password}
            . ";Catalog="             . $auth_hash->{Database}
            . ";RefreshToken="        . $auth_hash->{Port}
            . ";UseNativeQuery="      . $auth_hash->{Attribute_4}
            . ";SQLDialect="          . $auth_hash->{Attribute_5};
    }
    
    my $dbh = DBI->connect(
        $connection_string
      , $auth_hash->{Username}
      , $auth_hash->{Password}
    ) || $self->log->fatal( "Could not connect to " . $self->DB_TYPE . " database via ODBC API\n" . DBI->errstr );
    
    $self->dbh( $dbh );
    
}

sub get_fields_from_table {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $table_description = $self->fetch_bigquery_table_desc( $database, $schema, $table );
    
    my $return;
    
    foreach my $field ( @{$table_description->{schema}->{fields}} ) {
        push @{$return}, $field->{name};
    }
    
    if ( $options->{dont_sort} ) {
        return $return;
    } else {
        my @sorted = sort( @{$return} );
        return \@sorted;
    }
    
}

sub fetch_bigquery_table_desc {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    my $cached_bigquery_table_desc = $self->field_metadata_cache;
    
    if ( ! exists $cached_bigquery_table_desc->{ $table } ) {
        
        $cached_bigquery_table_desc->{ $table } = $self->[ $IDX_REST_CLIENT ]->desc_table(
            dataset_id  => $database
          , table_id    => $table
        );
        
        $self->field_metadata_cache( $cached_bigquery_table_desc );
        
    }
    
    return $cached_bigquery_table_desc->{ $table };
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $table_description = $self->fetch_bigquery_table_desc( $database, $schema, $table );
    
    my $return;
    
    foreach my $field ( @{$table_description->{schema}->{fields}} ) {
        
        $return->{ $field->{name} } =
        {
              COLUMN_NAME     => $field->{name}
            , DATA_TYPE       => $field->{type}
            , PRECISION       => undef
            , NULLABLE        => ( $field->{mode} eq 'REQUIRED' ? 0 : 1 )
            , COLUMN_DEFAULT  => undef
        };
        
    }
    
    return $return;
    
}

sub coalesce {
    
    my ( $self, $table, $expression, $string ) = @_;
    
    return "coalesce($expression,'$string')";
    
}

sub replace {
    
    my ( $self, $expression, $from, $to ) = @_;
    
    # This method constructs SQL to replace $from_char with $to_char in $expression
    
    return "replace($expression, $from, $to)";
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table , $options ) = @_;
    
    if ( ! $options->{dont_quote} ) {
        return $database . '.' . $table;
    } else {
        return '"' . $database . '"."' . $table . '"';
    }
    
}

sub _table_schema_from_Salesforce {
    
    my ( $self, $source_table_structure ) = @_;
    
    my @fields;
    
    my %sf_2_bq = (
        id          => 'STRING'
      , boolean     => 'BOOLEAN'
      , currency    => 'FLOAT'
      , percent     => 'FLOAT'
      , date        => 'DATE'
    ); # everything else is STRING
    
    foreach my $field ( @{$source_table_structure->{structure}->{fields}} ) {
        
        my $mapped_field_name = $field->{name};
        $mapped_field_name =~ s/[ -%\*#@\(\)~`+]/_/g;
        $mapped_field_name = decamelize( $mapped_field_name );
        
        $self->log->debug( "Mapping Salesforce field [" . $field->{name} . "] type [" . $field->{type}
                         . "] to BigQuery field [$mapped_field_name] type [" . ( $sf_2_bq{ $field->{type} } || 'STRING' ) . "]" );
        
        push @fields, {
            name        => $mapped_field_name
          , type        => ( $sf_2_bq{ $field->{type} } || 'STRING' )
          , description => $field->{label}
        };
        
    }
    
    return \@fields;
    
}

sub BIGQUERY_EXECUTE_SQL {

    my ( $self, $template_config_class ) = @_;

    # This method executes SQL, as we'd normally do via a $dbh->execute() call, but uses BigQuery's REST API
    # to submit it. Unfortunately this is required for full functionality:
    #  - maximumBillingTier is only exposed via the REST API, so some queries will simply fail if they require us raising the billing tier
    #  - google only even claim to "support" queries submitted via the REST API, where we've captured a job ID.
    #    ( Google refuses to even consider issues with BigQuery for SQL executed via Simba's ODBC driver )

    # First the required ones ( all others are optional and have default values )...
    my $dataset_id          = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $max_billing_tier    = $template_config_class->resolve_parameter( '#P_MAX_BILLING_TIER#' );

    my $template_text       = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );

    my $response;

    $self->log->debug( "Submitting SQL to Google via the only method they even claim to support ..." );

    $template_config_class->perf_stat_start( 'BigQuery SQL via REST API' );

    eval {

        # The docs say this creates a table, but it creates a FUCKING VIEW THAT YOU CAN'T QUERY!

        #$self->[ $IDX_REST_CLIENT ]->create_table( # return 1 (success) or 0 (error)
        #    project_id      => $self->[ $IDX_PROJECT_ID ]   # required if default project is not set
        #  , dataset_id      => $dataset_id                  # required if default project is not set
        #  , table_id        => $table_id                    # required
        #  , description     => undef                        # optional
        #  , expirationTime  => undef                        # optional
        #  , friendlyName    => undef                        # optional
        #  , schema          => undef                        # optional
        #  , view            => $sql                         # optional
        #);

        $response = $self->[ $IDX_REST_CLIENT ]->request(
              resource            => 'jobs'                       # BigQuery API resource
            , method              => 'insert'                     # BigQuery API method
            , project_id          => $self->[ $IDX_PROJECT_ID ]   # project_id
            , dataset_id          => $dataset_id                  # dataset_id
            , job_id              => undef                        # dafaq?
            , content             => {
                configuration => {
                    query => {
                          useLegacySql            => 'FALSE'
                        , maximumBillingTier      => $max_billing_tier
                        , query                   => $template_text
                        , priority                => 'INTERACTIVE'
                    }
                }
            }
            , data                => undef
        ) || die( $self->rest_client->errstr );

    };

    my $err = $@;

    if ( $response->{status}->{errors} ) {
        no warnings "uninitialized";
        $err .= "\n" . to_json( $response->{status}, { pretty => 1 } );
    }

    if ( $response->{error} ) {
        no warnings "uninitialized";
        $err .= "\n" . to_json( $response->{error}, { pretty => 1 } );
    }

    $template_config_class->perf_stat_stop( 'BigQuery SQL via REST API' );

    return {
          template_text       => $template_text
        , record_count        => ( $err ? 0 : 1 )
        , error               => $err
    };

}

sub BIGQUERY_TABLE_FROM_SQL {
    
    my ( $self, $template_config_class ) = @_;
    
    # First the required ones ( all others are optional and have default values )...
    my $dataset_id          = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $table_id            = $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_TABLE_NAME#" );
    my $max_billing_tier    = $template_config_class->resolve_parameter( '#P_MAX_BILLING_TIER#' );
    
    my $template_text = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    my $response;
    
    $self->log->debug( "attempting to create a bigquery table ..." );
    
    $template_config_class->perf_stat_start( 'BigQuery create table from SQL' );
    
    eval {
        
        # The docs say this creates a table, but it creates a FUCKING VIEW THAT YOU CAN'T QUERY!
        
        #$self->[ $IDX_REST_CLIENT ]->create_table( # return 1 (success) or 0 (error)
        #    project_id      => $self->[ $IDX_PROJECT_ID ]   # required if default project is not set
        #  , dataset_id      => $dataset_id                  # required if default project is not set
        #  , table_id        => $table_id                    # required
        #  , description     => undef                        # optional
        #  , expirationTime  => undef                        # optional
        #  , friendlyName    => undef                        # optional
        #  , schema          => undef                        # optional
        #  , view            => $sql                         # optional
        #);
        
        $response = $self->[ $IDX_REST_CLIENT ]->request(
            resource            => 'jobs'                       # BigQuery API resource
          , method              => 'insert'                     # BigQuery API method
          , project_id          => $self->[ $IDX_PROJECT_ID ]   # project_id
          , dataset_id          => $dataset_id                  # dataset_id
          , table_id            => $table_id                    # table_id
          , job_id              => undef                        # dafaq?
          , content             => {
                                        configuration => {
                                            query => {
                                                writeDisposition        => 'WRITE_TRUNCATE'
                                              , useLegacySql            => 'FALSE'
                                              , allowLargeResults       => 'TRUE'
                                              , maximumBillingTier      => $max_billing_tier
                                              , destinationTable        => {
                                                                                projectId   => $self->[ $IDX_PROJECT_ID ]
                                                                              , datasetId   => $dataset_id
                                                                              , tableId     => $table_id
                                                }
                                              , query                   => $template_text
                                              , priority                => 'INTERACTIVE'
                                            }
                                        }
            }
          , data                => undef
        ) || die( $self->rest_client->errstr );
        
    };
    
    my $err = $@;
    
    if ( $response->{status}->{errors} ) {
        no warnings "uninitialized";
        $err .= "\n" . to_json( $response->{status}, { pretty => 1 } );
    }

    if ( $response->{error} ) {
        no warnings "uninitialized";
        $err .= "\n" . to_json( $response->{error}, { pretty => 1 } );
    }

    $template_config_class->perf_stat_stop( 'BigQuery create table from SQL' );
    
    return {
        template_text       => $template_text
      , record_count        => ( $err ? 0 : 1 )
      , error               => $err
    };
    
}

sub BIGQUERY_DROP_TABLE {
    
    my ( $self, $template_config_class ) = @_;
    
    my $dataset_id  = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $table_id    = $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_TABLE_NAME#" );
    
    my $template_text = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    my $response;
    
    $self->log->debug( "attempting to drop a bigquery table ..." );
    
    $template_config_class->perf_stat_start( 'BigQuery drop table' );
    
    eval {
        
        $response = $self->[ $IDX_REST_CLIENT ]->drop_table(
            project_id      => $self->[ $IDX_PROJECT_ID ]
          , dataset_id      => $dataset_id
          , table_id        => $table_id
        ) || die( $self->[ $IDX_REST_CLIENT ]->errstr );
        
    };
    
    my $err = $@;
    
    $template_config_class->perf_stat_stop( 'BigQuery drop table' );
    
    return {
        template_text       => $template_text
      , record_count        => $response
      , error               => $err
    };
    
}

sub BIGQUERY_LOAD_FILE {
    
    my ( $self, $template_config_class ) = @_;
    
    # First the required ones ( all others are optional and have default values )...
    my $dataset_id  = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $table_id    = $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_TABLE_NAME#" );
    my $local_path  = $template_config_class->resolve_parameter( '#P_LOCAL_CSV_PATH#' )                  || $self->log->fatal( "Missing param #P_LOCAL_CSV_PATH#" );
    my $bucket_id   = $template_config_class->resolve_parameter( '#P_BUCKET_ID#' )                       || $self->log->fatal( "Missing param #P_BUCKET_ID#" );
    
    my $source_table_structure_name = $template_config_class->resolve_parameter( '#P_SOURCE_TABLE_STRUCTURE_NAME#' );
    
    my $target_schema;
    
    if ( $source_table_structure_name ) {
        
        my $source_table_structure = $template_config_class->resolve_parameter( '#Q_' . $source_table_structure_name . '#');
        my $schema_gen_method = '_table_schema_from_' . $source_table_structure->{type};
        
        if ( $self->can( $schema_gen_method ) ) {
            $target_schema = $self->$schema_gen_method( $source_table_structure );
        }
        
    }
    
    my $template_text = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    my $gs_path;
    
    if ( $local_path =~ /.*\/(.*)/ ) {
        $gs_path = 'gs://' . $bucket_id . '/' . $1;
    } else {
        $self->log->fatal( "Failed to parse local path: [$local_path]" );
    }
    
    # We have to push the file using 'gsutil' to Google Cloud Storage, as the method of loading
    # directly from a local file requires slurping the entire file into memory. I'm not sure
    # if this is a limitation of the library ( Google::BigQuery ) or not. It's easy and probably
    # more fault-tolerant to shell out to gsutil anyway.
    
    eval {
        
        $self->log->info( "Starting file upload to of [$local_path] to Google Cloud Storage [$gs_path]" );
        
        my @args = (
            'gsutil'
          , "cp"
          , $local_path
          , $gs_path
        );
        
        $template_config_class->perf_stat_start( 'File upload to Google Cloud Storage' );
        
        system( @args ) == 0
            or die( "gsutil upload failed: " . $? );
        
        $template_config_class->perf_stat_stop( 'File upload to Google Cloud Storage' );
        
        $self->log->info( "Starting BigQuery load from Google Cloud Storage" );
        
        $template_config_class->perf_stat_start( 'BigQuery load CSV from Google Cloud Storage' );
        
        my $field_delimiter = $template_config_class->resolve_parameter( '#P_FIELD_DELIMITER#' );
        
        if ( $field_delimiter eq '\001' ) {
            $field_delimiter = "\001";
        }
        
        my $source_format           = $template_config_class->resolve_parameter( '#P_SOURCE_FORMAT#' );
        my $quote                   = $template_config_class->resolve_parameter( '#P_QUOTE#' );
        my $skip_leading_rows       = $template_config_class->resolve_parameter( '#P_SKIP_LEADING_ROWS#' );
        my $allow_quoted_newlines   = $template_config_class->resolve_parameter( '#P_ALLOW_QUOTED_NEW_LINES#' ) ;
        
        if ( $source_format eq 'NEWLINE_DELIMITED_JSON' ) {
            # BigQuery barfs if we pass these in for JSON files :/
            $field_delimiter        = undef;
            $quote                  = undef;
            $skip_leading_rows      = undef;
            $allow_quoted_newlines  = undef;
        }
        
        $self->[ $IDX_REST_CLIENT ]->load(
            project_id          => $self->[ $IDX_PROJECT_ID ]
          , dataset_id          => $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )
          , table_id            => $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )
          , data                => $gs_path
          , allowJaggedRows     => $template_config_class->resolve_parameter( '#P_ALLOW_JAGGED_ROWS#' )
          , allowQuotedNewlines => $allow_quoted_newlines
          , createDisposition   => $template_config_class->resolve_parameter( '#P_CREATE_DISPOSITION#' )
          , encoding            => $template_config_class->resolve_parameter( '#P_ENCODING#' )
          , fieldDelimiter      => $field_delimiter
          , ignoreUnknownValues => $template_config_class->resolve_parameter( '#P_IGNORE_UNKNOWN_VALUES#' )
          , maxBadRecords       => $template_config_class->resolve_parameter( '#P_MAX_BAD_RECORDS#' )
          , quote               => $quote
          , schema              => ( $target_schema || $template_config_class->resolve_parameter( '#P_SCHEMA#' ) )
          , skipLeadingRows     => $skip_leading_rows
          , sourceFormat        => $template_config_class->resolve_parameter( '#P_SOURCE_FORMAT#' )
          , writeDisposition    => $template_config_class->resolve_parameter( '#P_WRITE_DISPOSITION#' )
        ) || die( $self->rest_client->errstr );
        
        $template_config_class->perf_stat_stop( 'BigQuery load CSV from Google Cloud Storage' );
        
        $self->log->info( "Deleting file [$gs_path] from Google Cloud Storage" );
        
        @args = (
            'gsutil'
          , "rm"
          , $gs_path
        );
        
        $template_config_class->perf_stat_start( 'BigQuery remove file from Google Cloud Storage' );
        
        system( @args ) == 0
            or $self->log->warn( "gsutil rm failed: " . $? );
        
        $template_config_class->perf_stat_stop( 'BigQuery remove file from Google Cloud Storage' );
        
        unlink( $local_path );
        
    };
    
    my $error = $@;
    
    return {
        template_text       => $template_text
      , record_count        => 0
      , error               => $error
    };
    
}

sub BIGQUERY_CREATE_TARGET_IF_NOT_EXISTS {
    
    my ( $self, $template_config_class ) = @_;
    
    my $source_database             = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_DB_NAME#" );
    my $source_table                = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_TABLE_NAME#" );
    my $target_database             = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $target_table                = $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_TABLE_NAME#" );
    my $source_table_structure_name = $template_config_class->resolve_parameter( '#P_SOURCE_TABLE_STRUCTURE_NAME#' )     || $self->log->fatal( "Missing param #P_SOURCE_TABLE_STRUCTURE_NAME" );

    my $template_text               = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    eval {
        
        $template_config_class->perf_stat_start( 'BigQuery check if table exists' );
        
        if ( ! $self->[ $IDX_REST_CLIENT ]->is_exists_table(
            project_id  => $self->[ $IDX_PROJECT_ID ]
          , dataset_id  => $target_database
          , table_id    => $target_table
        ) ) {
            
            $template_config_class->perf_stat_stop( 'BigQuery check if table exists' );
            
            $self->log->info( "Target table doesn't exist. Creating ..." );
            
            my $source_table_structure = $template_config_class->resolve_parameter( '#Q_' . $source_table_structure_name . '#');
            my $schema_gen_method = '_table_schema_from_' . $source_table_structure->{type};
            
            my $target_schema;
            
            if ( $self->can( $schema_gen_method ) ) {
                $target_schema = $self->$schema_gen_method( $source_table_structure );
            }
            
            $template_config_class->perf_stat_start( 'BigQuery create table' );
            
            $self->[ $IDX_REST_CLIENT ]->create_table(
                project_id      => $self->[ $IDX_PROJECT_ID ]
              , dataset_id      => $target_database
              , table_id        => $target_table
              , description     => "Mirror of $target_table from Salesforce"
              , expirationTime  => undef
              , friendlyName    => $target_table
              , schema          => $target_schema
              , view            => undef
            ) || die( $self->[ $IDX_REST_CLIENT ]->errstr );
            
            $template_config_class->perf_stat_stop( 'BigQuery create table' );
            
        } else {
            
            $template_config_class->perf_stat_stop( 'BigQuery check if table exists' );
            
        }
        
    };
    
    my $error = $@;
    
    if ( $error ) {
        $template_config_class->perf_stat_stop( 'BigQuery create table' );
    }
    
    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : 1 )
      , error               => $error
    };
    
}

sub BIGQUERY_MERGE_NEW_COLUMNS {
    
    my ( $self, $template_config_class ) = @_;
    
    my $source_database             = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_DB_NAME#" );
    my $source_table                = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_TABLE_NAME#" );
    my $target_database             = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $target_table                = $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_TABLE_NAME#" );
    
    my $template_text               = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    $template_config_class->perf_stat_start( 'BigQuery fetch source fields' );
    my $source_fields               = $self->get_fields_from_table( $source_database, undef, $source_table, { dont_sort => 1 } );
    $template_config_class->perf_stat_stop( 'BigQuery fetch source fields' );
    
    $template_config_class->perf_stat_start( 'BigQuery fetch target fields' );
    my $target_fields               = $self->get_fields_from_table( $target_database, undef, $target_table, { dont_sort => 1 } );
    $template_config_class->perf_stat_stop( 'BigQuery fetch target fields' );
    
    my $keys_string                 = $template_config_class->detokenize( '#P_KEYS#' );
    
    # Look for additional columns in the source.
    my @new_fields;
    
    my %target_fields_hash = map { $_ => 1 } @{$target_fields};
    
    foreach my $field ( @{$source_fields} ) {
        if ( ! exists $target_fields_hash{$field} ) {
            push @new_fields, $field;
        }
    }
    
    my $record_count = 0;
    
    if ( @new_fields ) {
        
        # Assemble a query that represents the new structure of the target table
        my @select_list;
        
        # First, include all the columns already in the target ( and let's keep things in order, so as not to surprise people who do "select * from table" )
        foreach my $field ( @{$target_fields} ) {
            push @select_list, 'TARGET.' . $field;
        }
        
        # Next, include all the columns in the source that aren't in the target
        foreach my $field ( @new_fields ) {
            push @select_list, 'SOURCE.' . $field;
        }
        
        my @joins;
        
        foreach my $key ( $template_config_class->split_comma_separated_columns( $keys_string, 1 ) ) {
            push @joins, 'TARGET.' . $key . ' = SOURCE.' . $key;
        }
        
        my $sql = "select\n    "
                . join( "\n  , ", @select_list )
                . "\nfrom\n"
                . "            " . $target_database . "." . $target_table . "      as TARGET\n"
                . "left join ( select * from " . $source_database . "." . $source_table . " where 0 = 1 ) as SOURCE\n"
                . " on " . join( "\n                    and ", @joins );
        
        $template_text .= "$sql";
        
        $template_config_class->perf_stat_start( 'BigQuery rebuild table' );
        
        eval {
            
            $self->[ $IDX_REST_CLIENT ]->request(
                resource            => 'jobs'                       # BigQuery API resource
              , method              => 'insert'                     # BigQuery API method
              , project_id          => $self->[ $IDX_PROJECT_ID ]   # project_id
              , dataset_id          => $target_database             # dataset_id
              , table_id            => $target_table                # table_id
              , job_id              => undef                        # dafaq?
              , content             => {
                                            configuration => {
                                                query => {
                                                    writeDisposition        => 'WRITE_TRUNCATE'
                                                  , useLegacySql            => 'FALSE'
                                                  , allowLargeResults       => 'TRUE'
                                                  , maximumBillingTier      => 4
                                                  , destinationTable        => {
                                                                                    projectId   => $self->[ $IDX_PROJECT_ID ]
                                                                                  , datasetId   => $target_database
                                                                                  , tableId     => $target_table
                                                    }
                                                  , query                   => $sql
                                                  , priority                => 'INTERACTIVE'
                                                }
                                            }
                }
              , data                => undef
            ) || die( $self->rest_client->errstr );
            
        };
        
        $template_config_class->perf_stat_stop( 'BigQuery rebuild table' );
        
        $record_count = 1;
        
    } else {
        
        $template_text .= " ... no changed detected";
        
    }
    
    my $error = $@;
    
    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : $record_count )
      , error               => $error
    };
    
}

sub BIGQUERY_CLONE_TABLE_STRUCTURE {
    
    my ( $self, $template_config_class ) = @_;
    
    my $source_database             = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_DB_NAME#" );
    my $source_table                = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_TABLE_NAME#" );
    my $target_database             = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $target_table                = $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_TABLE_NAME#" );
    
    my $template_text               = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    $template_config_class->perf_stat_start( 'BigQuery fetch source table schema' );
    my $table_description = $self->fetch_bigquery_table_desc( $source_database, undef, $source_table );
    $template_config_class->perf_stat_stop( 'BigQuery fetch source table schema' );
    
    $template_config_class->perf_stat_start( 'BigQuery create table' );
    eval {
        $self->[ $IDX_REST_CLIENT ]->create_table(
            project_id      => $self->[ $IDX_PROJECT_ID ]
          , dataset_id      => $target_database
          , table_id        => $target_table
          , description     => "Clone of $source_database.$source_table"
          , expirationTime  => undef
          , friendlyName    => $target_table
          , schema          => $table_description->{schema}->{fields}
          , view            => undef
        ) || die( $self->[ $IDX_REST_CLIENT ]->errstr );
    };
    $template_config_class->perf_stat_stop( 'BigQuery create table' );
    
    my $error = $@;
    
    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : 1 )
      , error               => $error
    };
    
}

sub BIGQUERY_TABLE_FROM_COLUMN_HEADINGS {
    
    my ( $self, $template_config_class ) = @_;
    
    my $target_database             = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )             || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    my $target_table                = $template_config_class->resolve_parameter( '#CONFIG_TARGET_TABLE_NAME#' )          || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_TABLE_NAME#" );
    
    my $column_type_map_json        = $template_config_class->resolve_parameter( '#P_COLUMN_TYPE_MAP#' );
    
    my $template_text               = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    my $main_file                   = $template_config_class->resolve_parameter( '#ENV_HARVEST_PATH#' );
    
    if ( $main_file =~ /(.*)\.gz/ ) {
        $main_file = $1;
    }
    
    my $headers_file                = $main_file . ".headers";
    
    eval {
        
        my ( $input, $output );
        
        my $column_type_map = decode_json( $column_type_map_json );
        
        $template_config_class->perf_stat_start( 'Parse Omniture column headers' );
        
        {
            no warnings 'uninitialized';
            open $input, "<utf8", $headers_file
                || die( "Failed to open headers file [$headers_file] for reading:\n" . $! );
        }
        
        my $csv_reader = Text::CSV->new(
        {
            quote_char              => '"'
          , binary                  => 0
          , eol                     => "\n"
          , sep_char                => "\t"
          , escape_char             => "\\"
          , quote_space             => 1
          , blank_is_undef          => 1
          , quote_null              => 1
          , always_quote            => 1
        } );
        
        my $column_definitions;
        
        if ( my $row = $csv_reader->getline( $input ) ) {
            
            foreach my $column ( @{$row} ) {
                
                my $this_column_type;
                
                if ( exists $column_type_map->{ $column } ) {
                    $this_column_type = $column_type_map->{ $column };
                    $self->log->debug( "Using provided column mapping: [$column] ==> [$this_column_type]" );
                } else {
                    $this_column_type = "STRING";
                    $self->log->debug( "Using default column type: [$column] ==> [$this_column_type]" );
                }
                
                push @{$column_definitions}
              , {
                    name        => $column
                  , type        => $this_column_type
                };
              
            }
            
        }
        
        close $input
            || die( "Failed to close headers file:\n" . $! );
        
        #unlink $headers_file;
        
        $template_config_class->perf_stat_stop( 'Parse Omniture column headers' );
        
        $template_config_class->perf_stat_start( 'BigQuery create table' );
        
        $self->[ $IDX_REST_CLIENT ]->create_table(
            project_id      => $self->[ $IDX_PROJECT_ID ]
          , dataset_id      => $target_database
          , table_id        => $target_table
          , description     => "Omniture temporary load table"
          , expirationTime  => undef
          , friendlyName    => $target_table
          , schema          => $column_definitions
          , view            => undef
        ) || die( $self->[ $IDX_REST_CLIENT ]->errstr );
        
        $template_config_class->perf_stat_stop( 'BigQuery create table' );
        
    };
    
    my $error = $@;
    
    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : 1 )
      , error               => $error
    };
    
}

sub BIGQUERY_TABLE_TO_CSV {
    
    my ( $self, $template_config_class ) = @_;
    
    my $source_database             = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_DB_NAME#' )            || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_DB_NAME#" );
    my $source_table                = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_TABLE_NAME#' )         || $self->log->fatal( "Missing CONFIG param #CONFIG_SOURCE_TABLE_NAME#" );
    
    my $template_text               = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    my $cloud_storage_bucket        = $template_config_class->resolve_parameter( '#P_GOOGLE_CLOUD_STORAGE_BUCKET#' )    || $self->log->fatal( "Missing param #P_GOOGLE_CLOUD_STORAGE_BUCKET#" );
    my $iterator_name               = $template_config_class->resolve_parameter( '#P_ITERATOR#' )                       || $self->log->fatal( "Missing param #P_ITERATOR#" );
    my $job_id                      = $template_config_class->resolve_parameter( '#ENV_JOB_ID#' );
    
    my $delimiter                   = $template_config_class->resolve_parameter( '#P_DELIMITER#' );
    
    # Swap string literal \t for the tab character
    if ( $delimiter eq '\t' ) {
        $delimiter = "\t";
    }
    
    $template_config_class->perf_stat_start( 'BigQuery extract table' );
    my $debug = 0;
    if ( ! $debug ) {
        
        eval {
            $self->[ $IDX_REST_CLIENT ]->extract(
                dataset_id          => $source_database
              , table_id            => $source_table
              , data                => [ $cloud_storage_bucket . "/sfx-job-" . $job_id . "_*.export" ]
              , compression         => $template_config_class->resolve_parameter( '#P_COMPRESSION#' )
              , destinationFormat   => $template_config_class->resolve_parameter( '#P_DESTINATION_FORMAT#' )
              , fieldDelimiter      => $delimiter
              , printHeader         => $template_config_class->resolve_parameter( '#P_PRINT_HEADER#' )
            ) || die( $self->[ $IDX_REST_CLIENT ]->errstr );
        };
        
    }
    
    $template_config_class->perf_stat_stop( 'BigQuery extract table' );
    
    my $gsutil_cmd = "gsutil ls " . $cloud_storage_bucket . "/sfx-job-" . $job_id . "_*";
    
    my @gs_files = `$gsutil_cmd`;
    
    my $all_records = [];
    my $counter = 0;
    
    foreach my $gs_location ( @gs_files ) {
        chomp $gs_location;
        my ( $filename, $directory, $suffix ) = fileparse( $gs_location ); # we don't store the path in the job_ctl table
        push @{$all_records}, {
            GS_PATH     => $gs_location
          , FILENAME    => $filename
        };
        $counter ++;
    }
    
    my $iterator = SmartAssociates::Iterator->new(
        $self->globals
      , $iterator_name
      , $all_records
    );

    $self->globals->ITERATOR( $iterator_name, $iterator );
    
    my $error = $@;
    
    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : $counter )
      , error               => $error
    };
    
}

sub BIGQUERY_TABLE_CLEANUP {
    
    my ( $self, $template_config_class ) = @_;
    
    my $target_database             = $template_config_class->resolve_parameter( '#CONFIG_TARGET_DB_NAME#' )            || $self->log->fatal( "Missing CONFIG param #CONFIG_TARGET_DB_NAME#" );
    
    my $template_text               = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );
    
    my $regex_pattern               = $template_config_class->resolve_parameter( '#P_REGEX_PATTERN#' ) || $self->log->fatal( "Missing param #P_REGEX_PATTERN#" );
    
    my @tables = $self->[ $IDX_REST_CLIENT ]->show_tables(
        dataset_id  => $target_database
      , maxResults  => 10000
    );
    
    my $compiled_regex = qr /$regex_pattern/;
    
    my $counter = 0;
    
    foreach my $table ( @tables ) {
        
        if ( $table =~ $compiled_regex ) {
            
            $self->log->info( "Dropping table [$table]" );
            
            my $response = $self->[ $IDX_REST_CLIENT ]->drop_table(
                dataset_id      => $target_database
              , table_id        => $table
            ) || $self->log->warn( $self->[ $IDX_REST_CLIENT ]->errstr );
            
            $counter ++;
            
        }
        
    }
    
    return {
        template_text       => $template_text
      , record_count        => $counter
      , error               => undef
    };
    
}

sub rest_client                     { return $_[0]->accessor( $IDX_REST_CLIENT,                     $_[1] ); }

1;
