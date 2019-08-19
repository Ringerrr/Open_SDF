package SmartAssociates::Database::Connection::Hive;

use strict;
use warnings;

use JSON;

use base 'SmartAssociates::Database::Connection::Base';

my $IDX_COLUMN_INFO_CACHE                       =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_DATABASE                                =  SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 1;

use constant FIRST_SUBCLASS_INDEX                                                                                   => 2;

use constant DB_TYPE                            => 'Hive';

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    my $connection_string =
          "dbi:ODBC:"
        . "DRIVER="       . $auth_hash->{ODBC_Driver}
        . ";Host="        . $auth_hash->{Host}
        . ";Port="        . $auth_hash->{Port};
    
    $self->[ $IDX_DATABASE ] = $auth_hash->{Ddatabase};
    
    return $self->SUPER::build_connection_string( $auth_hash, $connection_string );
    
}

#sub connect {
#    
#    my ( $self, $connection_string, $username, $password ) = @_;
#    
#    my $dbh = DBI->connect(
#        $connection_string
#      , $username
#      , $password
#      , {
#          FetchHashKeyName  => 'NAME_uc'
#        }
#    ) || $self->log->fatal( "Could not connect to " . $self->DB_TYPE . " database\n" . DBI->errstr );
#    
#    $dbh->do( "use " . $self->[ $IDX_DATABASE ] );
#    
#    $self->dbh( $dbh );
#    
#}

sub fetch_column_info_array {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $column_info_array;
    
    if ( ! $self->[ $IDX_COLUMN_INFO_CACHE ]->{$database}->{$table} ) {
        
        my $sth = $self->prepare( "describe $table" )
            || return;
        
        $self->execute( $sth )
            || return;
        
        while ( my $column_info = $sth->fetchrow_hashref ) {
            if ( $column_info->{col_name} eq '' ) { # the column info is followed by a blank line, then partition info
                last;
            }
            push @{$column_info_array}, $column_info;
        }
        
        $self->[ $IDX_COLUMN_INFO_CACHE ]->{$database}->{$table} = $column_info_array;
        
    }
    
    return $self->[ $IDX_COLUMN_INFO_CACHE ]->{$database}->{$table};
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $column_info_array = $self->fetch_column_info_array( $database, $schema, $table, $options );
    
    my $return;
    
    foreach my $column_info ( @{$column_info_array} ) {
        
        $return->{ $column_info->{col_name} } = {
            COLUMN_NAME     => $column_info->{col_name}
          , DATA_TYPE       => $column_info->{data_type}
          , PRECISION       => undef
          , NULLABLE        => 1
          , COLUMN_DEFAULT  => undef
        };
        
    }
    
    return $return;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    return $table;
    
}

sub HIVE_TABLE_FROM_COLUMN_HEADINGS {
    
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
        
        my @column_definitions;
        
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
                
                push @column_definitions
                  , $column . ( ' ' x ( 30 - length( $column ) ) ) . $this_column_type;
              
            }
            
        }
        
        close $input
            || die( "Failed to close headers file:\n" . $! );
        
        #unlink $headers_file;
        
        $template_config_class->perf_stat_stop( 'Parse Omniture column headers' );
        
        my $external_table_ddl = "create external table $target_table (\n"
            . "    " . join( "\n  , ", @column_definitions ) . "\n"
            . ")\n"
            . " ROW FORMAT\n"
            . " DELIMITED FIELDS TERMINATED BY '\t'\n"
            . " LINES TERMINATED BY '\n'\n"
            . " STORED AS TEXTFILE\n"
            . " LOCATION '" . $main_file . "'";
        
        $self->log->info( "External table DDL:\n" . $external_table_ddl );
        
        $template_config_class->perf_stat_start( 'Hive create table' );
        
        $self->dbh->do(
            $external_table_ddl
        ) || die( $self->dbh->errstr );
        
        $template_config_class->perf_stat_stop( 'Hive create table' );
        
    };
    
    my $error = $@;
    
    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : 1 )
      , error               => $error
    };
    
}

1;
