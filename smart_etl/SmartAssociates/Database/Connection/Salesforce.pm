package SmartAssociates::Database::Connection::Salesforce;

use strict;
use warnings;

use WWW::Salesforce;
use WWW::Mechanize;

use JSON;

use base 'SmartAssociates::Database::Connection::Base';

my $IDX_CURRENT_RESULTS                         =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_CURRENT_COLUMN_ORDER                    =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_CURRENT_SQL                             =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_ERRSTR                                  =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 3;
my $IDX_SESSION_ID                              =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 4;
my $IDX_MECH                                    =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 5;
my $IDX_SF_SERVER_URL                           =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 6;

use constant FIRST_SUBCLASS_INDEX                                                                                   => 7;

use constant SALESFORCE_API_VERSION             => '39.0';
use constant SALESFORCE_API_MAX_PAGE_SIZE       => 2000;

use constant DB_TYPE                            => 'Salesforce';

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    # As Salesforce doesn't use a traditional DBI-class driver,
    # we instead JSON-encode the credentials and return them
    # as the connection string, which then gets passed to
    # connect_do(), below.
    
    my $connection_string = encode_json( $auth_hash );
    
    return $connection_string;
    
}

sub connect_do {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    my $salesforce;
    
    eval {
        $salesforce = WWW::Salesforce->login(
            username => $auth_hash->{Username}
          , password => $auth_hash->{Password} . $auth_hash->{Attribute_1}
        );
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->log->fatal( "Could not connect to " . $self->DB_TYPE . "\n" . $err );
    }
    
    $self->dbh( $salesforce );
    
    if ( $salesforce->{sf_serverurl} =~ /(https:\/\/[\w\d]*\.salesforce\.com)/ ) {
        $self->[ $IDX_SF_SERVER_URL ] = $1;
    } else {
        $self->log->warn( "Failed to parse the salesforce server out of the sf_serverurl: [" . $salesforce->{sf_serverurl} . "]. Defaulting to [https://na1.salesforce.com]" );
        $self->[ $IDX_SF_SERVER_URL ] = "https://na1.salesforce.com";
    }

    my $hdr = $salesforce->get_session_header();
    $self->[ $IDX_SESSION_ID ] = ${$hdr->{_value}->[0]}->{_value}->[0];

    $self->[ $IDX_MECH ] = WWW::Mechanize->new();
    $self->[ $IDX_MECH ]->agent( 'Mozilla/5.0' );
    $self->[ $IDX_MECH ]->add_header( "Authorization" => "OAuth " . $self->[ $IDX_SESSION_ID ] );
    $self->[ $IDX_MECH ]->add_header( "X-PrettyPrint" => '1' );
    
}

sub column_names {
    
    my $self = shift;
    
    return $self->[ $IDX_CURRENT_COLUMN_ORDER ];
    
}

sub get_fields_from_table {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $sf_column_info = $self->fetch_salesforce_table_desc( $database, $schema, $table );
    
    my $return;
    
    foreach my $column ( @{$sf_column_info->{fields}} ) {
        push @{$return}, $column->{name};
    }
    
    return $return;
    
}

sub fetch_salesforce_table_desc {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_start( 'Salesforce describeSObject()' );

    $self->[ $IDX_MECH ]->get( $self->[ $IDX_SF_SERVER_URL ] . "/services/data/v" . SALESFORCE_API_VERSION . "/sobjects/" . $table . "/describe" );
    my $desc = decode_json( $self->[ $IDX_MECH ]->content );
    
    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_stop( 'Salesforce describeSObject()' );
    
    return $desc;
    
}

###############################################################################
# All the below methods are to bridge the gap between Salesforce and Perl's DBI
###############################################################################

sub prepare {
    
    my ( $self, $sql ) = @_;
    
    $self->log->debug( "Salesforce doesn't support preparing queries ... skipping" );
    
    $self->[ $IDX_CURRENT_SQL ] = $sql;
    
    # The prepare() method is supposed to return a $sth, but we return ourself and then
    # implement methods a $sth is supposed to ...
    
    return $self;
    
}

sub get {

    my ( $self, $request ) = @_;

    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_start( 'Salesforce REST API get()' );

    eval {
        $self->[ $IDX_MECH ]->get( $request ) || die( $self->[ $IDX_MECH ]->response->content );
        $self->[ $IDX_CURRENT_RESULTS ] = decode_json( $self->[ $IDX_MECH ]->content );
    };

    my $err = $@;

    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_stop( 'Salesforce REST API get()' );

    if ( $err ) {
        $self->[ $IDX_ERRSTR ] = $self->[ $IDX_MECH ]->response->content;
        return 0;
    } else {
        return 1;
    }

}

sub execute {
    
    my $self = shift;
    
    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )
    
    # We do this basically so we can log the line of the CALLER that requested a database execute()
    #  ... rather than just logging our own position inside this function
    
    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );
    
    # We only want to print a line break ( ie ====== ) of 60 characters or less
    my $sql_length = length( $self->[ $IDX_CURRENT_SQL ] );
    my $line_length = $sql_length < 60 ? $sql_length : 60;
    
    my $debug_log_line = "Executing SQL:\n" . ( '=' x $line_length )
                       . "\n" . $self->[ $IDX_CURRENT_SQL ]
                       . "\n" . ( '=' x $line_length );
    
    $self->log->debug(
        $debug_log_line
      , $line
      , $filename
    );
    
    # The results that come back from Salesforce via WWW:Salesforce have columns sorted alphabetically
    # We need to return them in the order specified in $sql, so we first parse the SQL and assemble an
    # array of columns that are in the correct order.
    #
    # I *was* using SQL::Statement from SQL::Parser to parse out the columns, but this is giving a lot of troubles, as
    # SOQL ( Salesforce something Query Language ) is not SQL. It seems to be easier and more robust just to parse the
    # columns out ourselves :/
    
    my $sql = $self->[ $IDX_CURRENT_SQL ];
    
    if ( $sql =~ /select(.*)from(.*)/sig ) {
        
        my $columns_string = $1;
        my @columns = split /,/, $columns_string;
        
        foreach my $column ( @columns ) {
            $column =~ s/\s//g;     # strip spaces
        }
        
        $self->[ $IDX_CURRENT_COLUMN_ORDER ] = \@columns;
        
    } else {
        
        $self->log->fatal( "Failed to parse select statement. We need to extract the column order from the SQL,"
                         . " as Salesforce returns the results in SOAP packages, and the column order gets mangled."
                         . " Either make the query simpler, or improve our parser." );
        
    }
    
    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_start( 'Salesforce query()' );

    # Replace spaces with pluses ( + )
    my $soql = $self->[ $IDX_CURRENT_SQL ];
    $soql =~ s/\s/\+/g;

    # If we've hit an error, we store it and return 0, which triggers our caller to call our errstr() method, eg:
    # $sth->execute() || die( $sth->errstr)
    my $status = $self->get( $self->[ $IDX_SF_SERVER_URL ] . "/services/data/v" . SALESFORCE_API_VERSION . "/query/?q=$soql" );

    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_stop( 'Salesforce query()' );
    
    if ( ! $status ) {
        return 0;
    }
    
    $self->log->info( "Salesforce indicated [" . $self->[ $IDX_CURRENT_RESULTS ]->{size} . "] records will be returned by this query" );
    
    return (
        $self->[ $IDX_CURRENT_RESULTS ]->{size} == 0
            ? &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED
            : $self->[ $IDX_CURRENT_RESULTS ]->{size}
    );
    
}

sub fetchall_arrayref {
    
    my ( $self, $key, $page_size ) = @_;
    
    my $this_return_page;
    
    my $results = $self->[ $IDX_CURRENT_RESULTS ];
    
    if ( ! $results ) {
        $self->log->debug( "fetchall_arrayref() called, but there is no results object in memory from a previous API operation" );
        return undef;
    }
    
    if ( ! exists $results->{records} ) {
        $self->log->warn( "fetchall_arrayref() called, and there is a results object in memory, but it contains no records array!" );
        return undef;
    } elsif ( ref $results->{records} ne 'ARRAY' ) {
        $self->log->warn( "fetchall_arrayref() called, and there is a results object in memory, and it contains a records object, but the records object is not an array! So close ... but no cookie" );
        return undef;
    }
    
    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_start( 'Parsing pages from Salesforce' );
    
    foreach my $record ( @{$results->{records}} ) {
        my $row;
        foreach my $column ( @{ $self->[ $IDX_CURRENT_COLUMN_ORDER ] } ) {
            my $this_value = $record->{$column};
            if ( ref $this_value eq 'ARRAY' ) {
                push @{$row}, $record->{$column}->[0]; # TODO: wtf are these arrays?
            } else {
                push @{$row}, $record->{$column};
            }
        }
        push @{$this_return_page}, $row;
    }
    
    $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_stop( 'Parsing pages from Salesforce' );
    
    if ( $results->{nextRecordsUrl} ) {
        $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_start( 'Fetching pages from Salesforce' );
        $self->get( $results->{nextRecordsUrl} );
        $self->globals->CURRENT_TEMPLATE_CONFIG->perf_stat_stop( 'Fetching pages from Salesforce' );
    } else {
        $self->[ $IDX_CURRENT_RESULTS ] = undef;
    }
    
    return $this_return_page;
    
}

sub finish {
    
    my $self = shift;
    
    $self->log->debug( "Salesforce sth finished" );
    
}

sub errstr {
    
    my $self = shift;
    
    return $self->[ $IDX_ERRSTR ];
    
}

########################################################################
# These methods implement entire template steps, and are called from the
# SmartAssociates::TemplateConfig::DatabaseMethod class
########################################################################

sub SALESFORCE_TABLE_STRUCTURE_TO_Q_PARAM {

    my ( $self, $template_config_class ) = @_;

    my $structure_name = $template_config_class->resolve_parameter( '#P_STRUCTURE_NAME#' );
    my $keys_name      = $template_config_class->resolve_parameter( '#P_KEYS_NAME#' );
    my $template_text  = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );

    my $source_table   = $template_config_class->resolve_parameter( '#CONFIG_SOURCE_TABLE_NAME#' )
        || $self->log->fatal( "Missing config param: CONFIG_SOURCE_TABLE_NAME" );

    my $structure;

    eval {

        $structure = $self->fetch_salesforce_table_desc( undef, undef, $source_table );

        # Scan for a field with type 'id'
        my $primary_key_field;

        foreach my $field ( @{$structure->{fields}} ) {
            if ( $field->{type} eq 'id' ) {
                $primary_key_field = $field->{name};
            }
        }

        my $query_parameters = $self->globals->Q_PARAMS;

        $query_parameters->{ $structure_name } = {
            type        => 'Salesforce'
          , structure   => $structure
        };

        $query_parameters->{ $keys_name } = $primary_key_field;

        $self->globals->Q_PARAMS( $query_parameters );

    };

    my $error = $@;

    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : 1 )
      , error               => $error
    };

}

1;
