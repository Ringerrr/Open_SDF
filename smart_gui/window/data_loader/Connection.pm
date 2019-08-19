package window::data_loader::Connection;

use warnings;
use strict;

use parent 'window::data_loader';

use Glib qw( TRUE FALSE );

use feature 'switch';

# This class implements base DB functionality that the data loader uses, eg
# assembling column definitions

sub generate {
    
    my ( $globals, $connection_type, $options ) = @_;
    
    my $object_class            = 'window::data_loader::' . $connection_type;
    
    my $connection_object;
    
    # Convert path name into relative path
    my $class_relative_path = $object_class;
    $class_relative_path =~ s/:/\//g;
    
    # We also need the '.pm' at the end ...
    $class_relative_path .= '.pm';
    
    my @all_paths = $globals->{local_db}->all_gui_paths;
    
    my $class_found;
    
    foreach my $include_path ( @all_paths ) {
        if ( -e $include_path . "/" . $class_relative_path ) {
            $class_found = 1;
            print "Loading Perl resource [" . $include_path . "/" . $class_relative_path . "] for connection type [$object_class]\n";
            eval {
                require $include_path . "/" . $class_relative_path;
            };
            my $error = $@;
            if ( $error ) {
                if ( $object_class eq 'Connection' ) {
                    dialog(
                    undef
                    , {
                          title       => "Compilation error!"
                        , type        => "error"
                        , text        => $error
                      }
                  );
                }
                return;
            }
        }
    }
    
    if ( $class_found ) {
        $connection_object = $object_class->new(
            $globals
          , $options
        );
    } else {
        $connection_object = window::data_loader::Connection->new(
            $globals
          , $options
        );
    }
    
    return $connection_object;
    
}

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    
    $self->{builder} = $options->{builder};
    
    bless $self, $class;
    
    return $self;
    
}

sub column_definitions_to_ddl {
    
    my ( $self, $records, $options ) = @_;
    
    my $ddl;
    my @columns;
    
    foreach my $record ( @{$records} ) {
        if ( $options->{quote_column_names} ) {
            push @columns, '"' . $record->{Name} . '" ' . $record->{Type};
        } else {
            push @columns, $record->{Name} . " " . $record->{Type};
        }
    }
    
    $ddl = "(\n    " . join( "\n  , ", @columns ) . "\n)";
    
    return $ddl;
    
}

sub column_mapper {
    
    # This method maps columns in window::data_loader to DB-specific columns
    
    my ( $self, $def ) = @_;
    
    my $type;
    
    # For columns that didn't have any data, assume a varchar(100)
    if ( ! $def->{type} ) {
        $def->{type} = "VARCHAR";
        $def->{max_length} = 100;
    }
    
    if ( $def->{type} =~ /CHAR/i ) {
        
        # The max_length is only the max we've seen so far, and I'm getting sick of re-reading files
        # with bigger & bigger number of rows read each time, just to get the max length. Let's just
        # go for 100 by default, and make it bigger if we've detected it already.
        
        my $calc_length;
        
        if ( $def->{max_length} > 100 ) {
            $calc_length = $def->{max_length};
        } elsif ( $def->{max_length} < 5 ) {
            $calc_length = $def->{max_length};
        } else {
            $calc_length = 100;
        }
        
        $type = $def->{type} . "($calc_length)";
        
    } elsif ( $def->{type} ne &window::data_loader::DATE && $def->{type} ne &window::data_loader::TIMESTAMP && $def->{type} ne &window::data_loader::INT ) {
        
        $type = $def->{type} . "(" . $def->{scale} . ")";
        
    } elsif ( $def->{type} eq &window::data_loader::INT ) {
        
        given ( $def->{scale} ) {
            when ( $_ == &window::data_loader::SCALE_BYTEINT )  { $type = 'BYTEINT' }
            when ( $_ == &window::data_loader::SCALE_TINYINT )  { $type = 'SMALLINT' }
            when ( $_ == &window::data_loader::SCALE_SMALLINT ) { $type = 'SMALLINT' }
            when ( $_ == &window::data_loader::SCALE_INT )      { $type = 'INT' }
            when ( $_ == &window::data_loader::SCALE_BIGINT )   { $type = 'BIGINT' }
        }
        
    } else {
        
        $type = $def->{type};
        
    }
    
    return $type;
    
}

sub create_table {
    
    my ( $self, $connection, $database, $schema, $table, $definition, $options ) = @_;
    
    my $db_schema_table = $connection->db_schema_table_string( $database, $schema, $table );
    
    if ( $options->{drop} ) {
        $connection->connection->do( "drop table " . $db_schema_table );
    }
    
    $connection->do( "create table $db_schema_table\n$definition" );
    
}

1;
