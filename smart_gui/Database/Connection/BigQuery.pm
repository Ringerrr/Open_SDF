package Database::Connection::BigQuery;

use parent 'Database::Connection';

use strict;
use warnings;

use Google::BigQuery;
use JSON;

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
      , Password        => "" # Private Key File ( PKC12 format )"
      , Database        => ""
      , Host_IP         => "Private Key File ( PKC12 or JSON format )"
      , Port            => ""
      , Attribute_1     => "Catalog ( Project )"
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => "UseNativeQuery" # UseNativeQuery
      , Attribute_5     => "SQLDialect( 0 or 1 ... 0=Legacy; 1=Standard )" # SQLDialect( 0 or 1 ... 0=Legacy; 1=Standard )"
      , ODBC_driver     => "ODBC Driver" # ODBC Driver"
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
        . ";KeyFilePath="         . $auth_hash->{Host}
        . ";Catalog="             . $auth_hash->{Attribute_1}
        #. ";RefreshToken="        . $auth_hash->{Port}
        . ";UseNativeQuery="      . $auth_hash->{Attribute_4}
        . ";SQLDialect="          . $auth_hash->{Attribute_5};

     # my $string =
     #      "dbi:ODBC:"
     #    . "DRIVER="               . $auth_hash->{ODBC_driver}
     #    . ";OAuthMechanism=0"
     #    . ";Email="               . $auth_hash->{Username}
     #    . ";KeyFilePath="         . $auth_hash->{Password}
     #    . ";Catalog="             . $auth_hash->{Attribute_1};

    # $string = 'dbi:ODBC:DRIVER=Simba ODBC Driver for Google BigQuery;OAuthMechanism=0;Email=bigquery-service-account@api-project-438416064020.iam.gserviceaccount.com;KeyFilePath=/opt/BigQuery/api-project-438416064020-bf17df0b92a5.p12;Catalog=bigquery-public-data;';

    # my $string = "";

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

        # Update 1: Google broke their ODBC driver and abandoned it. Now we rely on
        # my fork of the Rest library, available at: https://github.com/dankasak/Google-BigQuery
        # which has various fixes and additions.

        $self->{rest_connection} = $self->{connection} = Google::BigQuery::create(
            client_email        => $auth_hash->{Username},
            private_key_file    => $auth_hash->{Host},
            project_id          => $auth_hash->{Attribute_1},
            default_sql_mode    => '#standardSQL',
            verbose             => 1,
            debug               => 1
        ) || die( $@ );

        # $self->{rest_connection}->use_dataset( $auth_hash->{Attribute_1} );

        # Update 2: the ODBC driver is BAAAAAACK. Kinda.
        # The ODBC driver is only really NEEDED for the migration dashboard drill-down GUI, where we
        # use parameterized queries to fetch records. All other functionality "works" well enough to
        # not fail hopelessly, so we only really need to try building the ODBC connection if someone
        # has configured it.
        if ( $auth_hash->{ODBC_driver} ) {
            # Trigger a rebuild of the connection string
            $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
            $self->{connection} = DBI->connect(
                $auth_hash->{ConnectionString}
              , $auth_hash->{Username}
              , $auth_hash->{Password}
              #, $dbi_options_hash
            ) || die( $DBI::errstr );
        }
        
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

    $self->{_current_database} = $auth_hash->{Attribute_1};

    return 1;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    # $options contains:
    #{
    #    dont_quote      => 0
    #}

    # if ( $options->{dont_quote} ) {
        return $database . '.' . $schema . '.' . $table;
    # } else {
    #     return '"' . $database .'"."' . $table . '"';
    # }

}

sub limit_clause {
    
    my ( $self, $row_numbers ) = @_;
    
    return "limit $row_numbers";
    
}

sub has_schemas {
    
    my $self = shift;
    
    return 1;
    
}

sub fetch_database_list {
    
    my $self = shift;
    
    return sort( $self->{rest_connection}->show_projects );
    
}

sub fetch_schema_list {

    my $self = shift;

    return sort( $self->{rest_connection}->show_datasets );

}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return sort(
        $self->{rest_connection}->show_tables(
            project_id  => $database
          , dataset_id  => $schema
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
    
    if ( ! exists $self->{schema_cache}->{ $database }->{ $schema }->{ $table } ) {
        $self->{schema_cache}->{ $database }->{ $schema }->{ $table } = $self->{rest_connection}->desc_table(
            project_id  => $database
          , dataset_id  => $schema
          , table_id    => $table
        );
    }
    
    return $self->{schema_cache}->{ $database }->{ $schema }->{ $table };
    
}

sub fetch_column_info_array {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $table_description = $self->fetch_bigquery_table_desc( $database, $schema, $table );
    
    my $return;
    
    # Split out the DATA_TYPE and PRECISION ...
    foreach my $field ( @{$table_description->{schema}->{fields}} ) {
        
        push @{$return}
      , {
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
            project_id      => $database
          , dataset_id      => $schema
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

sub quote {

    my ( $self , $object ) = @_;

    return '`' . $object . '`';

}

# sub _model_to_table_ddl {
#
#     # BigQuery doesn't currently support crazy stuff like 'create table', so we have to use
#     # their leet haxor rest api calls to get things done. <sigh>
#
#     my ( $self, $mem_dbh, $object_recordset ) = @_;
#
#     my @warnings;
#
#     my $columns_and_mappers;
#
#     if ( ! defined $object_recordset->{schema_name} ) {
#         $columns_and_mappers = $mem_dbh->select(
#             "select * from table_columns where database_name = ? and schema_name is null and table_name = ? order by ID"
#           , [ $object_recordset->{database_name}, $object_recordset->{table_name} ]
#         );
#     } else {
#         $columns_and_mappers = $mem_dbh->select(
#             "select * from table_columns where database_name = ? and schema_name = ? and table_name = ? order by ID"
#           , [ $object_recordset->{database_name}, $object_recordset->{schema_name}, $object_recordset->{table_name} ]
#         );
#     }
#
#     my $table_definition;
#
#     my $counter;
#
#     foreach my $column_mapper ( @{$columns_and_mappers} ) {
#
#         my $column_type = $column_mapper->{target_column_type};
#         my $mangler;
#         my $mangled_return;
#
#         # Invoke the column mangler if a special complex type defined
#         if ( defined $column_type && $column_type =~ /{(.*)}/ ) {
#
#             $mangler = '_ddl_mangler_' . $1;
#
#             if ( $self->can( $mangler ) ) {
#
#                 $mangled_return = $self->$mangler( $column_type, $column_mapper->{column_precision} );
#
#             } else {
#
#                 $self->dialog(
#                     {
#                         title       => "Can't execute mangler"
#                       , type        => "error"
#                       , text        => "I've encountered the mangler type $column_type but there is no mangler"
#                                      . " by the name [$mangler] in target database's class"
#                     }
#                 );
#
#             }
#
#         }
#
#         my $final_type = exists $mangled_return->{type}
#                               ? $mangled_return->{type}
#                               : $column_mapper->{target_column_type};
#
#         push @{$table_definition}, {
#             name        => $column_mapper->{column_name}
#           , type        => $final_type
#         };
#
#         $counter ++;
#
#     }
#
#     my $table_json = encode_json( $table_definition );
#
#     return {
#         ddl         => $table_definition
#       , warnings    => join( "\n\n", @warnings )
#     };
#
# }

sub sql_to_sqlite {

    my ( $self, $sql, $progress_bar ) = @_;

    $progress_bar->set_text( "Executing BigQuery query ..." );

    Gtk3::main_iteration() while ( Gtk3::events_pending() );

    # This function pulls records from BigQuery and pushes them into a SQLite DB
    # Muhahahahahaha!

    my ( $aoh , $columns ) = $self->selectall_aoh_and_columns(
        query => $sql
    );

    my $sqlite_dbh = Database::Connection::SQLite->new(
        $self->{globals}
      , {
            location    => ":memory:"
        }
    );

    my $local_sql = "create table bigquery_table (\n    ";
    my ( @column_def_strings, @placeholders );

    foreach my $fieldname ( @{$columns} ) {
        push @column_def_strings, $fieldname . " text";
        push @placeholders, "?";
    }

    $local_sql .= join( "\n  , ", @column_def_strings ) . "\n)";

    print "\n$local_sql\n";

    $sqlite_dbh->do( $local_sql );

    $sqlite_dbh->{AutoCommit} = 0;

    $local_sql = "insert into bigquery_table (\n    " . join( "\n  , ", @{$columns} )
        . "\n) values (\n    " . join( "\n  , ", @placeholders ) . "\n)";

    my $insert_sth = $sqlite_dbh->prepare( $local_sql )
        || die( $sqlite_dbh->errstr );

    my $counter = 0;

    eval {
        foreach my $record ( @{$aoh} ) {
            $counter ++;
            if ( $counter % 500 == 0 ) {
                $sqlite_dbh->{AutoCommit} = 1;
                if ( $progress_bar ) {
                    $progress_bar->set_text( $counter );
                    $progress_bar->pulse;
                    Gtk3::main_iteration while ( Gtk3::events_pending );
                }
                $sqlite_dbh->{AutoCommit} = 0;
            }
            my $row;
            foreach my $column ( @{$columns} ) {
                if ( ! exists $record->{$column} ) {
                    die( "Didn't see column $column in returned recordset. You might have case sensitivity issues ( our implementation is case sensitive )" );
                }
                push @{$row}, $record->{$column};
            }
            $insert_sth->execute( @{$row} )
                || confess( $insert_sth->errstr );
        }
    };

    if ( $@ ) {
        $self->dialog(
            {
	            title   => "Error loading recordset to SQLite!"
              , type    => "error"
              , text    => $@
            }
        );
    }

    $sqlite_dbh->{AutoCommit} = 1;

    if ( $progress_bar ) {
        $progress_bar->set_text( "" );
        $progress_bar->set_fraction( 0 );
    }

    return ( $sqlite_dbh, "select * from bigquery_table" );

}

# sub is_sql_database {
#
#     my $self;
#
#     return FALSE;
#
# }

sub can_execute_ddl {
    
    my $self = shift;
    
    return TRUE;
    
}

sub has_odbc_driver {

    my $self = shift;

    return TRUE;

}

sub connection_browse_title {

    my $self = shift;

    return "Select a PK12 or JSON key file";

}

1;
