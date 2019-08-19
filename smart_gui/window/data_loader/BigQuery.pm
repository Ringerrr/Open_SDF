package window::data_loader::BigQuery;

use warnings;
use strict;

use parent 'window::data_loader::Connection';

use JSON;

use Glib qw( TRUE FALSE );

use feature 'switch';

sub column_mapper {
    
    # This method maps columns in window::data_loader to DB-specific columns
    
    my ( $self, $def ) = @_;
    
    my $type;
    
    # For columns that didn't have any data, assume a varchar(100)
    if ( ! $def->{type} ) {
        $def->{type} = "VARCHAR";
    }
    
    if ( $def->{type} =~ /CHAR/i ) {
        
        $type = 'STRING';
        
    } elsif ( $def->{type} eq &window::data_loader::INT ) {
        
        $type = 'INTEGER';
        
    } elsif ( $def->{type} eq &window::data_loader::DATE ) {
        
        $type = 'DATE';
        
    } elsif ( $def->{type} eq &window::data_loader::NUMERIC ) {
        
        $type = 'FLOAT';
        
    } else {
        
        $type = 'STRING';
        
    }
    
    return $type;
    
}

sub column_definitions_to_ddl {
    
    my ( $self, $records, $options ) = @_;
    
    my $ddl;
    my @columns;
    
    foreach my $record ( @{$records} ) {
        push @columns, {
            name    => $record->{Name}
          , type    => $record->{Type}
        };
    }
    
    $ddl = to_json( \@columns, { pretty => 1 } );
    
    return $ddl;
    
}

sub create_table {
    
    my ( $self, $connection, $database, $schema, $table, $definition, $options ) = @_;
    
    # Other databases will be using $connection->do() ... which has an eval and raises
    # gtk dialogs on errors
    
    eval {
        $connection->{rest_connection}->create_table(
            dataset_id      => $database
          , table_id        => $table
          , expirationTime  => undef
          , friendlyName    => $table
          , schema          => decode_json( $definition )
          , view            => undef
        ) || die( $connection->{rest_connection}->errstr );
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title       => "Error creating table"
              , type        => "error"
              , text        => $err
            }
        );
    }
    
}

1;