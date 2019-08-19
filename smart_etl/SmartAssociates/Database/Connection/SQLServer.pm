package SmartAssociates::Database::Connection::SQLServer;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Base';

use constant DB_TYPE            => 'SQLServer';

sub default_port {
    
    my $self = shift;
    
    return 1443;
    
}

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1], $_[2], $_[3], $_[4] );
    
    my $dbh = $self->dbh;
    
    $dbh->{LongReadLen} = 65535; # 64kb
    
    return $self;
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    no warnings 'uninitialized';
    
    my $string =
          "dbi:ODBC:"
        . "DRIVER="    . $auth_hash->{ODBC_driver}
        . ";server="   . $auth_hash->{Host}
        . ";Port="     . $auth_hash->{Port}
        . ";Database=" . $auth_hash->{Database}
        . ";UID="      . $auth_hash->{Username}
        . ";PWD="      . $auth_hash->{Password}
        . ";app=SmartDataFramework";
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

# Nothing is *passing* the table in here. If we need to resurrect this, we have to
# change all class methods to accept the table. We do our formatting/casting
# elsewhere these days - db formatting strings

#sub coalesce {
#    
#    my ( $self, $table, $expression, $string ) = @_;
#    
#    # SQL Server doesn't allow us to swap data types in a coalesce, so we have to
#    # convert to CHAR explicitly ...
#    
#    my $field_metadata = $self->fetch_column_info( undef, undef, $table );
#    
#    my $field_type = $field_metadata->{ uc($expression) }->{DATA_TYPE};
#    
#    if ( $field_type =~ /char/i ) {
#        
#        # No need to CAST, which is great, because SQL Server requires you to provide a length
#        # when casting, and that opens a can of worms ...
#        
#        return "coalesce([$expression],'$string')";
#        
#    } else {
#        
#        # For everything that's not CHAR, we DO need to cast it ... and in this case,
#        # it's safe to just use plan old VARHCAR ( as opposed to NVARCHAR ). For the
#        # length, 80 should be safe ... and is also the max length for many drivers
#        # before you have to tweak connection properties
#        
#        return "coalesce(cast(([$expression]) as VARCHAR(80)),'$string')";
#        
#    }
#    
#}

sub fetch_column_info {
     
    my ( $self, $db, $schema, $table ) = @_;
    
    # TODO: we're not using the $db here???
    
    my $cached_field_metadata = $self->field_metadata_cache;
    
    if ( ! exists $cached_field_metadata->{ $schema }->{ $table } ) {
      
        my $sth = $self->prepare(
            "select\n"
          . "    '[' + upper(COLUMN_NAME) + ']'             as COLUMN_NAME\n" # Microsoft's demo DBs use restricted words as columns!
          . "  , DATA_TYPE                                  as DATA_TYPE\n"
          . "  , case when upper(DATA_TYPE) like '%CHAR%'     then '(' + cast( CHARACTER_MAXIMUM_LENGTH as VARCHAR(5) ) + ')'\n"
          . "         when upper(DATA_TYPE) like 'FLOAT'      then '(' + cast( NUMERIC_PRECISION as VARCHAR(5) ) + ')'\n"
          . "         when upper(DATA_TYPE) = 'DECIMAL' or upper(DATA_TYPE) = 'MONEY'\n"
          . "              then '(' + cast( NUMERIC_PRECISION as VARCHAR(5) ) + ',' + cast( NUMERIC_SCALE as VARCHAR(5) ) + ')'\n"
          . "         else ''\n"
          . "    end as PRECISION\n"
          . "  , case when upper(IS_NULLABLE) = 'YES' then 1 else 0 end as NULLABLE\n"
          . "from\n"
          . "    INFORMATION_SCHEMA.COLUMNS\n"
          . "where\n"
          . "    upper(TABLE_SCHEMA) = ?\n"
          . "and upper(TABLE_NAME) = ?" );
        
        $self->execute( $sth, [ uc($schema), uc($table) ] ) ;
        
        my $field_metadata = $sth->fetchall_hashref( 'COLUMN_NAME' )
            || logFatal( "Failed to get field metadata from schema [$schema], table [$table]:\n"
                       . $sth->errstr );
        
        $sth->finish();
        
        $cached_field_metadata->{ $schema }->{ $table } = $field_metadata;
        
        $self->field_metadata_cache( $cached_field_metadata );
        
    }
    
    return $cached_field_metadata->{ $schema }->{ $table };
    
}

sub get_primary_key_columns {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    # NOTE: we're assuming the database has already been selected during connection.
    # I don't see why this would not be the case.
    
    my $sth = $self->prepare(
        "select\n"
      . "            upper(COLUMNS.name)    as COLUMN_NAME\n"
      . "from\n" 
      . "            sys.indexes            INDEXES\n"
      . "inner join  sys.index_columns      INDEX_COLUMNS\n"
      . "                                                     on INDEXES.object_id      = INDEX_COLUMNS.object_id\n"
      . "                                                    and INDEXES.index_id       = INDEX_COLUMNS.index_id\n" 
      . "inner join\n" 
      . "            sys.columns            COLUMNS\n"
      . "                                                    on INDEX_COLUMNS.object_id = COLUMNS.object_id\n"
      . "                                                   and INDEX_COLUMNS.column_id = COLUMNS.column_id\n" 
      . "inner join  sys.tables             TABLES\n"
      . "                                                    on INDEXES.object_id       = TABLES.object_id\n"
      . "inner join  sys.schemas            SCHEMAS\n"
      . "                                                    on TABLES.schema_id        = SCHEMAS.schema_id\n"
      . "where\n"
      . "            upper(SCHEMAS.name)    = ?\n"
      . "and         upper(TABLES.name)     = ?\n"
      . "and         INDEXES.is_primary_key = 1"
      . "order by\n"
      . "            INDEX_COLUMNS.index_column_id"
    );
    
    $self->execute( $sth, [ uc($schema), uc($table) ] ) ;
    
    my $rows = $sth->fetchall_arrayref();
    my $return;
    
    foreach my $row ( @{$rows} ) {
        push @{$return}, $row->[0];
    }
    
    $sth->finish();
    
    return $return;
    
}

# NP    Maybe call it some generic method name like this, and can then alter the approach/target function used as a prameter
#       eg $source_dbh->escape_expression ($expression, "replace"), or $source_dbh->escape_expression ($expression, "whack_with_brick")

sub escape_expression {
    
    # Just passing through. Of course would need to pull out the method if we decided to implement it this way
    
    return shift->replace( @_ );
    
}

# NP -  DB-specific replace function to construct expressions to replace/escape a given set
#       of characters; commmonly \r, \n, \t and (soon) and arbitray list of chars,
#       and maybe char sequences, depending on the target DB loader

sub escape {
    
    my ( $self, $expression, $escape_item ) = @_;
    
    my $escape_char = '\\'; # this ends up being a single backslash
    
#    my $esc_chars   = [
#        92     # \          ... keep in mind ... the order is important
#      , 10     # LF
#      , 13     # CF
#      , 44     # ,
#      , 34     # ""
#    ];
    
    $expression = "replace( $expression, $escape_item, '$escape_char' + $escape_item )";
    
    # Finally remove any ASCII(0) ( NULL ) characters
#    $expression = "replace( $expression, char(0), '' )";
    
    return $expression;
    
}

sub replace {
    
    my ( $self, $expression, $from, $to ) = @_;
    
    # This method constructs SQL to replace $from_char with $to_char in $expression
    return "replace( $expression, $from, $to )";
    
}

# NP Create an expression concatenating all columns/expressions to be jammed toegether into one call to the msd5sum algorythm on source (along with primary key)
    # Quick example of using the fn_repl_hash_binary function on adventureworks 2014
    # SELECT SalesOrderID, [Customer].CustomerID, OrderDate, TotalDue,
    #    master.sys.fn_repl_hash_binary ( cast (
    #                                       cast([Customer].CustomerID as varchar(20)) + CONVERT(VARCHAR(19), OrderDate, 120) + cast (TotalDue as varchar(22) )
    #                                   as varbinary(max)))
    #                            as big_arse_checksum
    #      FROM [AdventureWorks2014].[Sales].[SalesOrderHeader] inner join [AdventureWorks2014].[Sales].[Customer] on [AdventureWorks2014].[Sales].[Customer].CustomerID = [AdventureWorks2014].[Sales].[SalesOrderHeader].CustomerID
# NP Note - to make it generic, concatenate doesn't alias the result for now, you need to wrap it. We could add an $alias/$suffix option I guess

sub concatenate {
    
    my $self    = shift;
    my $options = shift;
    
    # NP TODO Change use/require of Data::Dumper to only when debug flag set etc
    #warn "In source concat routine.. Have been passed in expressions: " . Dumper ($options);
    
    # Note: This concatentation of all cols excludes the primary keys, they'll be added on our return, and used for uniqueness V speed
    
    # As in above example SQL statement, some columns/expressions are going to need to be cast into varchar to be able to be concatenated
    # We've passed the type in for now to keep the logic encapsulated in here, but will look at ways of expanding formatted_select shortly
    
    my @all_expressions = ();
    
    foreach my $exp ( @{ $options->{expressions} } ) {
        
        # For most cases, just the expression or col name
        # NOTE  When an expression is passed back from formatted_select, it is aliased.. We need to
        #       remove that here for now (see note at top of formatted_select about flag for not aliasing)
        #       because  they are just going to be cast, and concatenated as part of a larger expression
        #       (I don't like doing this here - especially because the column alias could be quoted etc)
        
        my $this_expression = $exp->{expression};
        
        $this_expression =~ s/(\s+?|^)as\s+?\w+//i;
        
        # Now, match any numbery type thingies (from type list DK has in oracle.pm) and alter if needed
        if ( $exp->{type_code} ~~ [2, 3] && $exp->{precision} ) {
            # For types such as money
            $this_expression = "cast ( cast ($exp->{expression} as numeric($exp->{precision}->{precision}, $exp->{precision}->{scale}) ) as VARCHAR(20) )";
        }
        elsif ( $exp->{type_code} ~~ [-7..-5, 2..8] ) {
            
            # no need to alias individual cols/expressions
            $this_expression = "cast ($exp->{expression} as VARCHAR(20))"; # TODO: is this safe? It's for integer types - it quite possibly IS safe
            
        } elsif ( $exp->{type_code} == 12 ) {
            # http://www.i18nqa.com/debug/table-iso8859-1-vs-windows-1252.html
            my $convert_cp1252_to_latin9 = { 0x80 => 0xA4, 0x8A => 0xA6, 0x8C => 0xBC, 0x8E => 0xB4, 0x9A => 0xA8, 0x9C => 0xBD, 0x9E => 0xB8, 0x9F => 0xBE };
            while ((my $from_code_point, my $to_code_point) = each %$convert_cp1252_to_latin9) {
                $this_expression = "REPLACE($this_expression, ASCII($from_code_point), ASCII($to_code_point))";
            }
        }
        
        # TODO: optimise - don't coalesce NOT NULL columns
        push @all_expressions, "coalesce($this_expression,'')";
        
    }
    
    my $exp = join ' + ', @all_expressions;
    
    return $exp;
    
}

# NP - Moved this out to its own routine so we can use concat more generically
# TODO These generic routines could probably live in the parent class

sub create_expression_md5sum {
    
    my $self    = shift;
    my $options = shift;
    
    my $exp = $self->concatenate( $options );
    
    # NP TODO also, maybe give the hash some identifying characterits, like table name appended.. But needs more thought in case it's not a simple table
    # NP Note that the sqlserver md5 hex hash is mixed case (and varbinary), where the nz one is all uppercase (needs to have '0x' prepended, and is varchar)..
    
    # So will it be better performance to do a case-insensitive match, or convert to upper-case on load on NZ?
    # TODO It's addressed here for now, but discussed with DK, can change based on performance decisions
    #   https://msdn.microsoft.com/en-us/library/ms187928.aspx
    # This is one way to cast it, and avoid nvarchar
     
    my $ret = "convert(varchar(34), master.sys.fn_repl_hash_binary ( cast ( ($exp) as varbinary ( max ) )), 1) as HASH_VALUE";
     
    # NP TODO As per comment from Huw 26082015, perhaps try to "export" master.sys.fn_repl_hash_binary so that it doesn't have to be fully-qualified to call
    
    return $ret;
    
}

# NP - This takes the primary key col(s) and concatenates them if more than one
# TODO for single pk cases, esp if some sort of int, would be better to leave untouched for distribution

sub create_comparison_pk_col {
    
    my $self = shift;
    my $cols = shift;
    
    # NP the arrayref of arrayref is for multi-column primary keys
    # We also know the type here, so can make decisions about how to cast
    
    my @final_column_expressions = ();
    
    foreach my $pk_col_def ( @{$cols} ) {
        
        #foreach my $pk_col_def
        push @final_column_expressions, $pk_col_def->[2];  # IE the COLUMN_NAME return from get_primary_key_columns
        
    }
    
    my $ret = join ",", @final_column_expressions;
    
}

sub does_table_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select TABLE_NAME from " . $self->db_schema_table_string( $database, "INFORMATION_SCHEMA", "TABLES" ) . "\n"
      . "where  TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = '" . $schema . "' and TABLE_NAME = '" . $table . "'";

    return $sql;

}

sub does_schema_not_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    # TODO: MG: flip to "not exists":
    my $sql = "select schema_name from " . $self->db_schema_table_string( $database, "INFORMATION_SCHEMA", "SCHEMATA" ) . "\n"
      . "where schema_name = '" . $schema . "'";

    return $sql;

}

1;
