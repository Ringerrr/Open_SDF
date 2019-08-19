package Database::Connection::BigQuery;

use parent 'Database::Connection';

use strict;
use warnings;

use Google::BigQuery;

use Glib qw | TRUE FALSE |;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'char2hexint';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'BigQuery';

sub connection_label_map {

    my $self = shift;
    
    return {
        Username        => "ClientEmail"
      , Password        => "Private Key File ( PKC12 format )"
      , Database        => "Catalog"
      , Host_IP         => ""
      , Port            => ""
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => "UseNativeQuery"
      , Attribute_5     => "SQLDialect( 0 or 1 ... 0=Legacy; 1=Standard )"
      , ODBC_driver     => "ODBC Driver"
    };
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    no warnings 'uninitialized';
    
    my $string =
          "dbi:ODBC:"
        . "DRIVER="               . $auth_hash->{ODBC_driver}
        . ";OAuthMechanism=0"
        . ";Email="               . $auth_hash->{Username}
        . ";KeyFilePath="         . $auth_hash->{Password}
        . ";Catalog="             . $auth_hash->{Database}
        . ";RefreshToken="        . $auth_hash->{Port}
        . ";UseNativeQuery="      . $auth_hash->{Attribute_4}
        . ";SQLDialect="          . $auth_hash->{Attribute_5};
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub default_port {

    my $self = shift;

    return -1;

}

sub connect_do {
    
    my ( $self, $auth_hash, $options_hash ) = @_;
    
    print "connecting ...\n";
    
    eval {
        
        # We make use of TWO libraries for Google BigQuery.
        # One uses their Rest API, and implements things like
        # fetching database, table & column metadata well.
        # The other uses their ODBC driver, and handles
        # executing queries well. Obviously, both are incomplete,
        # but combined, they just barely give us enough functionality
        # to do what we need.
        
        $self->{rest_connection} = Google::BigQuery::create(
            client_email        => $auth_hash->{Username},
            private_key_file    => $auth_hash->{Password},
            project_id          => $auth_hash->{Database},
        ) || die( $@ );
        
        # Trigger a rebuild of the connection string
        $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
        
        $self->{connection} = DBI->connect(
            $auth_hash->{ConnectionString}
          , $auth_hash->{Username}
          , $auth_hash->{Password}
          #, $dbi_options_hash
        ) || die( $DBI::errstr );
        
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title   => "Failed to connect to database"
              , type    => "error"
              , text    => $err
            }
        );
        return undef;
    }
    
    
    return 1;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    # $options contains:
    #{
    #    dont_quote      => 0
    #}

    if ( $options->{dont_quote} ) {
        return $database . '.' . $table;
    }else{
        return '"' . $database .'"."' . $table . '"';
    }

}

sub limit_clause {
    
    my ( $self, $row_numbers ) = @_;
    
    return "limit $row_numbers";
    
}

sub has_schemas {
    
    my $self = shift;
    
    return 0;
    
}

sub fetch_database_list {
    
    my $self = shift;
    
    return sort( $self->{rest_connection}->show_datasets );
    
}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return sort(
        $self->{rest_connection}->show_tables(
            dataset_id  => $database
          , maxResults  => 10000
        )
    );
    
}

sub fetch_view_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return ();
    
}

sub fetch_function_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return ();
    
}
sub fetch_procedure_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return ();
    
}

sub fetch_bigquery_table_desc {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    if ( ! exists $self->{schema_cache}->{ $database }->{ $table } ) {
        $self->{schema_cache}->{ $database }->{ $table } = $self->{rest_connection}->desc_table(
            dataset_id  => $database
          , table_id    => $table
        );
    }
    
    return $self->{schema_cache}->{ $database }->{ $table };
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $table_description = $self->fetch_bigquery_table_desc( $database, $schema, $table );
    
    my $return;
    
    # Split out the DATA_TYPE and PRECISION ...
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

sub fetch_field_list {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $table_description = $self->fetch_bigquery_table_desc( $database, $schema, $table );
    
    my $return;
    
    foreach my $field ( @{$table_description->{schema}->{fields}} ) {
        push @{$return}, $field->{name};
    }
    
    return $return;
    
}

sub drop_table {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    eval {
        my $response = $self->{rest_connection}->drop_table(
            dataset_id      => $database
          , table_id        => $table
        ) || die( $self->{rest_connection}->errstr );
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title   => "Failed to drop table"
              , type    => "error"
              , text    => $err
            }
        );
        return undef;
    }
    
    return 1;
    
}

sub _model_to_table_ddl {
    
    # BigQuery doesn't currently support crazy stuff like 'create table', so we have to use
    # their leet haxor rest api calls to get things done. <sigh>
    
    my ( $self, $mem_dbh, $object_recordset ) = @_;
    
    my @warnings;
    
    my $columns_and_mappers;
    
    if ( ! defined $object_recordset->{schema_name} ) {
        $columns_and_mappers = $mem_dbh->select(
            "select * from table_columns where database_name = ? and schema_name is null and table_name = ? order by ID"
          , [ $object_recordset->{database_name}, $object_recordset->{table_name} ]
        );
    } else {
        $columns_and_mappers = $mem_dbh->select(
            "select * from table_columns where database_name = ? and schema_name = ? and table_name = ? order by ID"
          , [ $object_recordset->{database_name}, $object_recordset->{schema_name}, $object_recordset->{table_name} ]
        );
    }
    
    my $table_definition;
    
    my $counter;
    
    foreach my $column_mapper ( @{$columns_and_mappers} ) {
        
        my $column_type = $column_mapper->{target_column_type};
        my $mangler;
        my $mangled_return;
        
        # Invoke the column mangler if a special complex type defined
        if ( defined $column_type && $column_type =~ /{(.*)}/ ) {
            
            $mangler = '_ddl_mangler_' . $1;
            
            if ( $self->can( $mangler ) ) {
                
                $mangled_return = $self->$mangler( $column_type, $column_mapper->{column_precision} );
                
            } else {
                
                $self->dialog(
                    {
                        title       => "Can't execute mangler"
                      , type        => "error"
                      , text        => "I've encountered the mangler type $column_type but there is no mangler"
                                     . " by the name [$mangler] in target database's class"
                    }
                );
                
            }
            
        }
        
        my $final_type = exists $mangled_return->{type}
                              ? $mangled_return->{type}
                              : $column_mapper->{target_column_type};
        
        push @{$table_definition}, {
            name        => $column_mapper->{column_name}
          , type        => $final_type
        };
        
        $counter ++;
        
    }
    
    my $table_json = encode_json( $table_definition );
    
    return {
        ddl         => $table_definition
      , warnings    => join( "\n\n", @warnings )
    };
    
}

sub can_execute_ddl {
    
    my $self = shift;
    
    return FALSE;
    
}

sub has_odbc_driver {

    my $self = shift;

    return TRUE;

}

1;
