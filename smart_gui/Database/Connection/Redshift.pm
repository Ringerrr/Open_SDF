package Database::Connection::Redshift;

use parent 'Database::Connection::Postgres';

use strict;
use warnings;

use feature 'switch';

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'unicodes';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'Redshift';

# Greenplum is an MPP database, based on Postgres ( forked around the 8.x days )
# As a result, we can *mostly* use our Postgres class, but there are a few differences,
# which are handled below

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
    
    if ( $distribution_keys ) {
        
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

1;
