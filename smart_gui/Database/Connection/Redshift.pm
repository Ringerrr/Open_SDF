package Database::Connection::Redshift;

use parent 'Database::Connection::Postgres';

use strict;
use warnings;

use feature 'switch';

use Glib qw | TRUE FALSE |;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'unicodes';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'Redshift';

# Redshift is an MPP database, based on Postgres ( forked around the 8.x days )
# As a result, we can *mostly* use our Postgres class, but there are a few differences,
# which are handled below

sub connection_label_map {

    my $self = shift;
    
    return {
        Username        => "DbUser"
      , Password        => "Password"
      , Database        => "Database"
      , Host_IP         => "Server"
      , Port            => "Port"
      , Attribute_1     => "SSL Mode"
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => "ODBC Driver"
    };
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    no warnings 'uninitialized';
    
    my $string =
          "dbi:ODBC:"
        . "DRIVER="               . $auth_hash->{ODBC_driver}
#        . ";DbUser="              . $auth_hash->{Username}
#        . ";Password="            . $auth_hash->{Password}
        . ";Database="            . $auth_hash->{Database}
        . ";Server="              . $auth_hash->{Host}
        . ";Port="                . $auth_hash->{Port}
        . ";SSLMode="             . $auth_hash->{Attribute_1};
    
    print "Redshift.pm assembled connection string: $string\n";
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub connect_post {
    
    my ( $self , $auth_hash , $options_hash ) = @_;
    
    $self->{connection}->{LongReadLen} = 65535 * 1024; # 64MB
    $self->{connection}->{LongTruncOk} = 1;
    $self->{connection}->{odbc_ignore_named_placeholders} = 1;
    
    return;
    
}

sub fetch_server_version {

    my $self = shift;
 
    my $version_aoh = $self->select( "select version() as version_no" );
    $self->{server_version} = $$version_aoh[0]->{VERSION_NO};

    return $self->{server_version};
   
}

sub fetch_materialized_view_list {

    my ( $self, $database, $schema ) = @_;

    # Greenplum doesn't support materialized views ...

    return ();

}

sub generate_db_load_command {
    
    my ( $self, $options ) = @_;
    
    # This should be a perfect copy of the Postgres method, but without the ( brackets )
    # around the CSV options ( after the 'with' directive )
    
    # options looks like:
#    {
#        file_path       => $file_path
#      , remote_client   => $remote_client
#      , null_value      => $null_value
#      , delimiter       => $delimiter
#      , skip_rows       => $skip_rows
#      , quote_char      => $quote_char
#      , encoding        => $encoding
#      , date_style      => $date_style
#      , date_delim      => $date_delim
#      , escape_char     => $escape_char
#      , database        => $target_database
#      , schema          => $target_schema
#      , table           => $target_table
#    }
    
    my $copy_command;
    
    if ( $options->{date_style} ) {
        $self->do( "set datestyle = 'ISO, " . $options->{date_style} . "'" );
    }
    
    $copy_command .= "copy " . $self->db_schema_table_string( $options->{database}, $options->{schema}, $options->{table} )
                   . "\nfrom " . ( $options->{remote_client} ? "STDIN\n" : "'" . $options->{file_path} . "'" )
                   . "\nwith\n";
    
    my @options_array;
    
    if ( $options->{skip_rows} ) {
        push @options_array, "header";
    }
    
    if ( $options->{delimiter} ) {
        push @options_array, "delimiter as E'" . $options->{delimiter} . "'";
    }
    
    # TODO: ???
    #if ( $options->{encoding} ) {
    #    $copy_command .= ", encoding'" . $options->{encoding} . "'";
    #}
    
    if ( $options->{escape_char} ) {
        push @options_array, "escape as E'" . ( $options->{escape_char} eq '\\' ? '\\' . $options->{escape_char} : $options->{escape_char} ) . "'";
    }
    
    if ( $options->{quote_char} ) {
        push @options_array, "csv quote as E'" . $options->{quote_char} . "'";
    }
    
    $copy_command .= join( "\n    ", @options_array ) . "\n;";
    
    return $copy_command;
    
}

sub _model_to_table_ddl {
    
    my ( $self, $mem_dbh, $object_recordset ) = @_;
    
    # The parent class ( ie Postgres ) assembles the main 'create table' DDL, and then we append the distribution clause to the end ...
    
    my $return = $self->SUPER::_model_to_table_ddl( $mem_dbh, $object_recordset );
    
    my $sql = $return->{ddl};
    
    my @warnings = split( /\n\n/, $return->{warnings} ); # parent class joins warnings, so we split any back into an array ...
    
    # First we check if there are any distribution keys. For source databases that support distribution, we obviously want to use the same key for Greenplum
    my $distribution_keys = $mem_dbh->_model_to_distribution_key_structure( $object_recordset->{database_name}, $object_recordset->{schema_name}, $object_recordset->{table_name} );
    
    # If there were no distribution keys, we fall back to using the primary keys
    if ( ! $distribution_keys ) {
        $distribution_keys = $mem_dbh->_model_to_primary_key_structure( $object_recordset->{database_name}, $object_recordset->{schema_name}, $object_recordset->{table_name} );
    }
    
    my @pk_items;
    
    if ( @{ $distribution_keys } ) {
        
        $sql .= " distkey ( ";
        
        my $column_count = 0;
        
        foreach my $row ( @{$distribution_keys} ) {
            # Redshift has a limit of ONE for the number of columns in a distribution key:
            # http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
            $column_count ++;
            if ( $column_count > 1 ) {
                push @warnings, "Skipping > 1 distribution key components for ["
                    . $object_recordset->{database} . "." . $object_recordset->{schema_name} . "." . $object_recordset->{table_name} . "]";
                last;
            }
            push @pk_items, $row->{column_name};
        }
        
        $sql .= join( " , ", @pk_items ) . " )\n";
        
    } else {
        
        $sql .= " diststyle even\n" ;
        
    }
    
    $sql .= ";";
    
    return {
        ddl         => $sql
      , warnings    => join( "\n\n", @warnings )
    };
    
}

sub fetch_all_indexes {

    my ( $self, $database, $schema ) = @_;

    # NOTE: this query will currently only search in the CURRENT database
    # ( there appears to be no way to include the database in the query )
    # To work around this, we create another connection, using our own
    # auth hash, but replacing the database

    my $connection = $self;

    if ( $database ne $self->{database} ) {

        my $auth_hash = { %{$self->{auth_hash}} }; # *copy* the hash, don't copy a *reference* to it ...

        $auth_hash->{Database} = $database;

        $connection = Database::Connection::Redshift->new(
            $self->{globals}
          , $auth_hash
        );

    }

    my $sth;

    eval {

        $sth = $connection->prepare(
            "select\n"
          . "    *\n"
          . "from\n"
          . "    pg_table_def\n" # https://docs.aws.amazon.com/redshift/latest/dg/r_PG_TABLE_DEF.html
          . "where\n"
          . "    schemaname = ?"
        ) or die( $connection->errstr );

    };

    my $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching indexes"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    print "\n" . $sth->{Statement} . "\n";

    eval {

        $sth->execute( $schema )
            or die( $sth->errstr );

    };

    $err = $@;

    if ( $err ) {

        $self->dialog(
            {
                title       => "Error fetching indexes"
              , type        => "error"
              , text        => $err
            }
        );

        return;

    }

    my $return;

    # We're creating a structure that looks like:

    #$return = {
    #    "INDEX_NAME"  => {
    #        IS_PRIMARY         => 0 or 1
    #      , IS_UNIQUE          => 0 or 1
    #      , IS_DISTIBUTION_KEY => 0 or 1
    #      , TABLE_NAME         => "TABLE_NAME"
    #      , COLUMNS            => [ "COL_1", "COL_2", etc ... ]
    #    }
    #};

    while ( my $row = $sth->fetchrow_hashref ) {


        my $distkey_column;

        if ( $row->{diststyle} =~ /KEY\((.*)\)/ ) {
            $distkey_column = $1;
            $return->{ $row->{table} }->{IS_PRIMARY}           = 0;
            $return->{ $row->{table} }->{IS_UNIQUE}            = 0;
            $return->{ $row->{table} }->{IS_DISTRIBUTION_KEY}  = 1;
            $return->{ $row->{table} }->{TABLE_NAME}           = $row->{table};
            push @{ $return->{ $row->{table} }->{COLUMNS} }    , $distkey_column;
        }

    }

    # # End of fetching indexes.
    # # Now we fetch organisation keys, and merge them in with the indexes
    #
    # eval {
    #
    #     $sth = $connection->prepare(
    #         "select\n"
    #             . "     s.DATABASE\n"
    #             . "   , o.SCHEMA\n"
    #             . "   , o.TABLENAME\n"
    #             . "   , o.ATTNAME\n"
    #             . "from _v_table_organize_column as o\n"
    #             . "     inner join _v_schema as s\n"
    #             . "         on o.SCHEMA = s.SCHEMA\n"
    #             . "where\n"
    #             . "      s.DATABASE = ?\n"
    #             . "and   o.SCHEMA   = ?\n"
    #             . "order by o.ORGSEQNO"
    #     ) or die( $self->errstr );
    #
    # };
    #
    # $err = $@;
    #
    # if ( $err ) {
    #
    #     $self->dialog(
    #         {
    #             title       => "Error fetching organisation keys"
    #                 , type        => "error"
    #             , text        => $err
    #         }
    #     );
    #
    #     return;
    #
    # }
    #
    # print "\n" . $sth->{Statement} . "\n";
    #
    # eval {
    #
    #     $sth->execute( $database, $schema )
    #         or die( $sth->errstr );
    #
    # };
    #
    # $err = $@;
    #
    # if ( $err ) {
    #
    #     $self->dialog(
    #         {
    #             title       => "Error fetching organisation keys"
    #                 , type        => "error"
    #             , text        => $err
    #         }
    #     );
    #
    #     return;
    #
    # }
    #
    # while ( my $row = $sth->fetchrow_hashref ) {
    #
    #     my $key_name = $row->{TABLENAME} . "_-_ORGKEY";
    #
    #     $return->{ $key_name }->{IS_PRIMARY}           = 0;
    #     $return->{ $key_name }->{IS_UNIQUE}            = 0;
    #     $return->{ $key_name }->{IS_DISTRIBUTION_KEY}  = 0;
    #     $return->{ $key_name }->{IS_ORGANISATION_KEY}  = 1;
    #     $return->{ $key_name }->{TABLE_NAME}           = $row->{TABLENAME};
    #
    #     push @{ $return->{ $key_name }->{COLUMNS} }
    #         , $row->{ATTNAME};
    #
    # }

    return $return;

}

sub generate_migration_import_sequence {
    
    my ( $self, $options ) = @_;
    
    # This method generates a sequence of steps to load INTO Postgres, using the
    # "parallel migration" method. This approach creates a unix pipe, and has
    # one process ( the source of the migration ) export data to the pipe,
    # while THIS process ( the target of the migration ) loads data from the pipe.
    
    # It uses the Postgres COPY command, and pushes data from a remote client
    # to the Postgres server. ie it opens the file in Perl and writes to the client.
    
    # $options contains:
    #{
    #    processing_group_name                   => $processing_group_name
    #  , target_connection_name                  => $target_connection_name
    #  , skip_rows                               => $skip_rows # ( $source_use_bcp ? 0 : 1 )
    #};

    # TODO: Redshift copy / bulk load template

#    $self->{globals}->{windows}->{'window::migration_wizard'}->log( "Creating PARALLEL_GREENPLUM_COPY_TO_TARGET step" );
#
#    $self->{globals}->{windows}->{'window::main'}->autogen(
#        {
#            group  => {
#                        PROCESSING_GROUP_NAME           => $options->{processing_group_name}
#                      , PROCESSING_GROUP_DESCRIPTION    => "Import data into $options->{target_connection_name}"
#                      , TAGS                            => "migration"
#            }
#          , config => {
#                PROCESSING_GROUP_NAME       => $options->{processing_group_name}
#              , TEMPLATE_NAME               => 'PARALLEL_GREENPLUM_COPY_TO_TARGET'
#              , CONNECTION_NAME             => $options->{target_connection_name}
#              , SOURCE_DB_NAME              => ""
#              , SOURCE_SCHEMA_NAME          => ""
#              , SOURCE_TABLE_NAME           => ""
#              , TARGET_DB_NAME              => "#P_MIGRATION_DATABASE#"
#              , TARGET_SCHEMA_NAME          => "#P_MIGRATION_SCHEMA#"
#              , TARGET_TABLE_NAME           => "#P_MIGRATION_TABLE#"
#              , PARENT_SEQUENCE_ORDER       => 0
#            }
#          , param_value => { }
#        }
#      , 1 # reset sequence order
#    );
    
}

sub has_odbc_driver {
    
    my $self = shift;
    
    return TRUE;
    
}

1;
