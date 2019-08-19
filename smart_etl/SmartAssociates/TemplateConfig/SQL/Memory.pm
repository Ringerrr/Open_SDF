package SmartAssociates::TemplateConfig::SQL::Memory;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::SQL';

use constant    BLOCK_SIZE      => 5000;

use constant VERSION                            => '1.1';

sub handle_executed_sth {
    
    my ( $self, $sth ) = @_;
    
    my $target_table = $self->resolve_parameter( '#P_TARGET_TABLE#' )
        || $self->log->fatal( "SQL::Memory templates must define the [#P_TARGET_TABLE#] parameter" );
    
    my $columns_string = $self->resolve_parameter( '#P_COLUMNS#' )
        || $self->log->fatal( "SQL::Memory templates must define the [#P_COLUMNS#] parameter" );
    
    my $mem_dbh = $self->processing_group->target_database(
        &SmartAssociates::Database::Connection::Memory::DB_TYPE
      , &SmartAssociates::Database::Connection::Memory::DB_TYPE
      , &SmartAssociates::Database::Connection::Memory::DB_TYPE
    );
    
    # To generate this hash, run 'dump_sql_types.pl' ( in the same directory as etl.pl )
    # Note that it sets ALL types to 'TEXT'
    
    my $type_code_to_sqlite_type_mappings = {
        -11    => 'TEXT'    # SQL_GUID
      , -10    => 'TEXT'    # SQL_WLONGVARCHAR
      ,  -9    => 'TEXT'    # SQL_WVARCHAR
      ,  -8    => 'TEXT'    # SQL_WCHAR
      ,  -5    => 'INTEGER' # SQL_BIGINT
      ,  -7    => 'INTEGER' # SQL_BIT
      ,  -6    => 'INTEGER' # SQL_TINYINT
      ,  -4    => 'TEXT'    # SQL_LONGVARBINARY
      ,  -3    => 'TEXT'    # SQL_VARBINARY
      ,  -2    => 'BLOB'    # SQL_BINARY
      ,  -1    => 'TEXT'    # SQL_LONGVARCHAR
      ,   0    => 'TEXT'    # SQL_UNKNOWN_TYPE
      ,   1    => 'TEXT'    # SQL_CHAR
      ,   2    => 'REAL'    # SQL_NUMERIC   ... note: NZ is reporting DECIMALS as this type
      ,   3    => 'INTEGER' # SQL_DECIMAL   ... note: Oracle is reporting INTEGERS as this type
      ,   4    => 'INTEGER' # SQL_INTEGER
      ,   5    => 'INTEGER' # SQL_SMALLINT
      ,   6    => 'REAL'    # SQL_FLOAT
      ,   7    => 'REAL'    # SQL_REAL
      ,   8    => 'REAL'    # SQL_DOUBLE
      ,   9    => 'TEXT'    # SQL_DATETIME
      ,   9    => 'TEXT'    # SQL_DATE
      ,  10    => 'TEXT'    # SQL_INTERVAL
      ,  10    => 'TEXT'    # SQL_TIME
      ,  11    => 'TEXT'    # SQL_TIMESTAMP
      ,  12    => 'TEXT'    # SQL_VARCHAR
      ,  16    => 'INTEGER' # SQL_BOOLEAN
      ,  17    => 'TEXT'    # SQL_UDT
      ,  18    => 'TEXT'    # SQL_UDT_LOCATOR
      ,  19    => 'TEXT'    # SQL_ROW
      ,  20    => 'TEXT'    # SQL_REF
      ,  30    => 'BLOB'    # SQL_BLOB
      ,  31    => 'BLOB'    # SQL_BLOB_LOCATOR
      ,  40    => 'BLOB'    # SQL_CLOB
      ,  41    => 'BLOB'    # SQL_CLOB_LOCATOR
      ,  50    => 'TEXT'    # SQL_ARRAY
      ,  51    => 'TEXT'    # SQL_ARRAY_LOCATOR
      ,  55    => 'TEXT'    # SQL_MULTISET
      ,  56    => 'TEXT'    # SQL_MULTISET_LOCATOR
      ,  91    => 'TEXT'    # SQL_TYPE_DATE
      ,  92    => 'TEXT'    # SQL_TYPE_TIME
      ,  93    => 'TEXT'    # SQL_TYPE_TIMESTAMP
      ,  94    => 'TEXT'    # SQL_TYPE_TIME_WITH_TIMEZONE
      ,  95    => 'TEXT'    # SQL_TYPE_TIMESTAMP_WITH_TIMEZONE
      , 101    => 'TEXT'    # SQL_INTERVAL_YEAR
      , 102    => 'TEXT'    # SQL_INTERVAL_MONTH
      , 103    => 'TEXT'    # SQL_INTERVAL_DAY
      , 104    => 'TEXT'    # SQL_INTERVAL_HOUR
      , 105    => 'TEXT'    # SQL_INTERVAL_MINUTE
      , 106    => 'TEXT'    # SQL_INTERVAL_SECOND
      , 107    => 'TEXT'    # SQL_INTERVAL_YEAR_TO_MONTH
      , 108    => 'TEXT'    # SQL_INTERVAL_DAY_TO_HOUR
      , 109    => 'TEXT'    # SQL_INTERVAL_DAY_TO_MINUTE
      , 110    => 'TEXT'    # SQL_INTERVAL_DAY_TO_SECOND
      , 111    => 'TEXT'    # SQL_INTERVAL_HOUR_TO_MINUTE
      , 112    => 'TEXT'    # SQL_INTERVAL_HOUR_TO_SECOND
      , 113    => 'TEXT'    # SQL_INTERVAL_MINUTE_TO_SECOND
    };
    
    # First we need to create the memory table
    
#    my @columns = $self->split_comma_separated_columns( $columns_string );
    my $column_names = $sth->{NAME};
    my $column_types = $sth->{TYPE};
    
    my @columns_definitions;
    
    my $counter = 0;
    
    foreach my $column ( @{$column_names} ) {
        
        $self->log->debug(
            "Column: [$column] being reported as type code: [" . $$column_types[ $counter ] . "]."
          . " Selecting SQLite type: [" . $type_code_to_sqlite_type_mappings->{ $$column_types[ $counter ] } . "]" );
        
        push  @columns_definitions
            , $column . '   ' . $type_code_to_sqlite_type_mappings->{ $$column_types[ $counter ] };
        
        $counter ++;
        
    }
    
    my $sql = "create table $target_table (\n    "
        . join( "\n  , ", @columns_definitions )
        . "\n)";
    
    $mem_dbh->do( $sql );
    
    $sql = "insert into $target_table (\n    "
        . join( "  , ", @{$column_names} )
        . "\n) values (\n    "
        . "?," x ( @{$column_names} - 1 ) . "?"
        . "\n)";
    
    my $insert_sth = $mem_dbh->prepare( $sql );
    
    $counter = 0;
    
    $mem_dbh->begin_work;
    
    while ( my $record = $sth->fetchrow_arrayref ) {
        
        $insert_sth->execute( @{$record} )
            || $self->log->fatal( $insert_sth->errstr );
		
        $counter ++;
        
        if ( $counter % BLOCK_SIZE == 0 ) {
            $mem_dbh->commit;
            $mem_dbh->begin_work;
        }
        
    }
    
    $mem_dbh->commit;
    
}

1;
