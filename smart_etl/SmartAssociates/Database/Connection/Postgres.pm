package SmartAssociates::Database::Connection::Postgres;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Base';

use constant FIRST_SUBCLASS_INDEX   => SmartAssociates::Database::Connection::Base::FIRST_SUBCLASS_INDEX + 0;

use constant DB_TYPE                => 'Postgres';

sub default_port {
    
    my $self = shift;
    
    return 5432;
    
}

sub build_connection_string {
    
    my ( $self, $credentials ) = @_;
    
    no warnings 'uninitialized';
    
    my $connection_string =
          "dbi:Pg:dbname=" . $credentials->{Database}
        . ";host="         . $credentials->{Host}
        . ";port="         . $credentials->{Port};
    
    return $connection_string;
    
}

sub connect_pre {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    $auth_hash->{ConnectionString} = '';

    if ( $auth_hash->{ConnectionName} eq 'METADATA' ) {
        $self->log->info( "Forcing UPPER CASE hash keys for METADATA connection ..." );
        $options_hash->{dbi_options_hash} = {
            FetchHashKeyName => 'NAME_uc'
        };
    }

    return ( $auth_hash , $options_hash );
        
}

sub get_fields_from_table {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    my $fields = $self->SUPER::get_fields_from_table( $database, $schema, $table );
    
    my @reserved_words = qw | select from table column index user update where
        ABORT DEC LEADING RESET DECIMAL LEFT REUSE AGGREGATE DECODE LIKE RIGHT ALIGN DEFAULT LIMIT ROWS ALL DEFERRABLE LISTEN ROWSETLIMIT ALLOCATE DESC LOAD RULE ANALYSE DISTINCT LOCAL SEARCH ANALYZE DISTRIBUTE LOCK SELECT AND DO MATERIALIZED SEQUENCE ANY ELSE MINUS SESSION_USER AS END MOVE SETOF ASC EXCEPT NATURAL SHOW
        BETWEEN EXCLUDE NCHAR SOME BINARY EXISTS NEW SUBSTRING BIT EXPLAIN NOT SYSTEM BOTH EXPRESS NOTNULL TABLE CASE EXTEND NULL THEN
        CAST EXTERNAL NULLIF TIES CHAR EXTRACT NULLS TIME CHARACTER FALSE NUMERIC TIMESTAMP CHECK FIRST NVL TO CLUSTER FLOAT NVL2 TRAILING
        COALESCE FOLLOWING OFF TRANSACTION COLLATE FOR OFFSET TRIGGER COLLATION FOREIGN OLD TRIM COLUMN FROM ON TRUE CONSTRAINT FULL ONLINE UNBOUNDED
        COPY FUNCTION ONLY UNION  CROSS GENSTATS OR UNIQUE CURRENT GLOBAL ORDER USER CURRENT_CATALOG GROUP OTHERS USING CURRENT_DATE HAVING OUT VACUUM
        CURRENT_DB IDENTIFIER_CASE OUTER VARCHAR CURRENT_SCHEMA ILIKE OVER VERBOSE CURRENT_SID IN OVERLAPS VERSION CURRENT_TIME INDEX PARTITION VIEW CURRENT_TIMESTAMP
        INITIALLY POSITION WHEN CURRENT_USER INNER PRECEDING WHERE CURRENT_USERID INOUT PRECISION WITH CURRENT_USEROID INTERSECT PRESERVE WRITE DEALLOCATE INTERVAL PRIMARY RESET
        INTO REUSE CTID  OID XMIN CMIN XMAX CMAX TABLEOID ROWID DATASLICEID CREATEXID DELETEXID
    |;
    
    foreach my $field ( @{$fields} ) {
        
        if ( grep(/^$field$/i, @reserved_words) ) {
            $field = '"' . $field . '"';
        }
        
    }
    
    my $all_fields_cache = $self->fields_cache;
    $all_fields_cache->{ $database . "." . $schema . "." . $table } = $fields;
    
    $self->fields_cache( $all_fields_cache );
    
    return $fields;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table , $options ) = @_;
    
    # Note: Postgres will throw 'cross db references not supported' if we include the database
    # We should already be connected to the correct database ...
    
    if ( ! $options->{dont_quote} ) {

        if ( defined $schema ) {
            return $schema . '.' . $table;
        } elsif ( defined $table ) {
            return $table;
        } else {
            $self->log->fatal( "db_schema_table_string() wasn't passed anything" );
        }
        
    } else {

        if ( defined $schema ) {
            return '"' . $schema . '"."' . $table . '"';
        } elsif ( defined $table ) {
            return '"' . $table . '"';
        } else {
            $self->log->fatal( "db_schema_table_string() wasn't passed anything" );
        }
        
    }
    
}

sub fetch_column_info {

    my ( $self, $db, $schema, $table ) = @_;

    $table =~ s/"//g; # Strip out quotes - we quote reserved words in get_fields_from_table()

    my $cached_field_metadata = $self->field_metadata_cache;

    if ( ! exists $cached_field_metadata->{ $schema }->{ $table } ) {

        use constant COLUMN_NUMBER  => 0;
        use constant COLUMN_NAME    => 1;
        use constant DATA_TYPE      => 2;
        use constant PRECISION      => 3;
        use constant NULLABLE       => 4;
        use constant COLUMN_DEFAULT => 5;

        my $sql = "select\n"
          . "    f.attnum AS number\n"
          . "  , f.attname as COLUMN_NAME\n"
          . "  , pg_catalog.format_type(f.atttypid,f.atttypmod) AS DATA_TYPE\n"
          . "  , NULL as PRECISION\n" # gets injected below
          . "  , case when f.attnotnull then 0 else 1 end as NULLABLE\n"
          . "from\n"
          . "               pg_attribute   f\n"
          . "    inner join pg_class       c\n"
          . "                                  on c.oid = f.attrelid\n"
          . "    inner join pg_type        t\n"
          . "                                  on t.oid = f.atttypid\n"
          . "    left join  pg_attrdef     d\n"
          . "                                  on d.adrelid = c.oid\n"
          . "                                 and d.adnum = f.attnum\n"
          . "    left join  pg_namespace   n\n"
          . "                                  on n.oid = c.relnamespace\n"
          . "    left join  pg_constraint   p\n"
          . "                                  on p.conrelid = c.oid\n"
          . "                                 and f.attnum = ANY (p.conkey)\n"
          . "    left join  pg_class       g\n"
          . "                                  on p.confrelid = g.oid\n"
          . "where\n"
          . "     ( c.relkind = 'r'::char or c.relkind = 'f'::char )\n" # r = table ( relation? ) , f = foreign table
          . " and n.nspname = lower(?)\n"
          . " and c.relname = lower(?)\n"
          . " and f.attnum > 0\n"
          . "order by\n"
          . "     number\n";

        my $sth = $self->prepare(
            $sql
        ) || return;

        $self->execute( $sth, [ $schema, $table ] );

        my $column_info = $sth->fetchall_arrayref;

        my $field_metadata;

        # Split out the DATA_TYPE and PRECISION ...
        foreach my $column ( @{$column_info} ) {

            # if ( $column->[ DATA_TYPE ] =~ /([\w\s]*)(\(.*\))/ ) {//
            if ( $column->[ DATA_TYPE ] =~ /([\w\s]*)\((.*)\)/ ) {
                ( $column->[ DATA_TYPE ] , $column->[ PRECISION ] ) = ( $1 , $2 );
            }

            $field_metadata->{ $column->[ COLUMN_NAME ] } =
            {
                  COLUMN_NAME     => $column->[ COLUMN_NAME ]
                , DATA_TYPE       => $column->[ DATA_TYPE ]
                , PRECISION       => $column->[ PRECISION ]
                , NULLABLE        => $column->[ NULLABLE ]
                , COLUMN_DEFAULT  => $column->[ COLUMN_DEFAULT ]
            };

        }


        $cached_field_metadata->{ $schema }->{ $table } = $field_metadata;

        $self->field_metadata_cache( $cached_field_metadata );

    }

    return $cached_field_metadata->{ $schema }->{ $table };

}

sub create_expression_md5sum {

    my $self    = shift;
    my $options = shift;

    my $exp = $self->concatenate( $options );

    return "'0x' || upper( md5( $exp ) ) as HASH_VALUE";

}

sub coalesce {
    
    my ( $self, $table, $expression, $string ) = @_;
    
    # TODO: use field metadata for type & size
    return "coalesce(cast($expression as varchar(5000)),'$string')";
    
}

sub replace {
    
    my ( $self, $expression, $from, $to ) = @_;
    
    $self->log->fatal( "replace() not yet implemented for Postgres!" );
    
    # This method constructs SQL to replace $from_char with $to_char in $expression
#    return "SYSTEM..REPLACE( $expression, $from, $to )";
    
}

sub concatenate {

    my $self    = shift;
    my $options = shift;

    my @all_expressions = ();

    foreach my $exp ( @{ $options->{expressions} } ) {

        # For most cases, just the expression or col name
        # NOTE  When an expression is passed back from formatted_select, it is aliased.. We need to
        #       remove that here for now (see note at top of formatted_select about flag for not aliasing)
        #       because  they are just going to be cast, and concatenated as part of a larger expression
        #       (I don't like doing this here - especially because the column alias could be quoted etc)

        my $this_expression = $exp->{expression};

        # NP TODO - Remove this seeing there's no aliasing from formatted_select
        #$tmp =~ s/(\s+?|^)as\s+?\w+//i;

        # Now, match any numbery type thingies (from type list DK has in oracle.pm) and alter if needed

        if ( $exp->{type_code} ~~ [-7..-5, 2..8] ) {

            # no need to alias individual cols/expressions; they'll be concated as part of larger expression
            $this_expression = "cast( $this_expression as VARCHAR(20) )";

        }

        # TODO: optimise - don't coalesce NOT NULL columns
        push @all_expressions, "coalesce($this_expression,'')";

    }

    my $return = join ' || ', @all_expressions;

    return $return;

}

sub encrypt_expression {
    
    my ( $self, $expression ) = @_;
    
    return 'encode( digest( ' . $expression . "::VARCHAR, 'sha256' ), 'base64' )";
    
}

sub POSTGRES_8_X_LOAD_REMOTE {

    my ( $self, $template_config_class ) = @_;

    return $self->POSTGRES_LOAD_REMOTE( $template_config_class );

}

sub POSTGRES_9_X_LOAD_REMOTE {

    my ( $self, $template_config_class ) = @_;

    return $self->POSTGRES_LOAD_REMOTE( $template_config_class );

}

sub POSTGRES_LOAD_REMOTE { # the SQL itself is different, but the custom stuff we do works for both 8_x and 9_x style COPY commands ...

    my ( $self, $template_config_class ) = @_;

    my $file_path           = $template_config_class->resolve_parameter( '#P_FILE_NAME#' )               || $self->log->fatal( "Missing #P_FILE_NAME#" );

    $self->log->debug( "POSTGRES_LOAD_REMOTE opening path: [$file_path]" );

    open my $csv_file, "<$file_path"
        || $self->log->fatal( "Couldn't open file [$file_path]\n" . $! );

    $self->log->debug( " ... path opened ..." );
    
    my $template_text               = $template_config_class->detokenize( $template_config_class->template_record->{TEMPLATE_TEXT} );

    my $dbh     = $template_config_class->target_database->dbh;

    my $counter;

    eval {

        # We *could* hand off $template_text to the TemplateConfig::SQL class, which usually executes SQL,
        # but it calls $sth->finish() immediately after, and we don't want that ( we call pg_putcopydata() ).
        # It seems easier / cleaner to just execute it directly here ...

        $template_config_class->perf_stat_start( 'Postgres prepare for remote load' );
        $dbh->do( $template_text ); # this will execute the 'copy' command ( do() can die(), but we're in an eval block )
        $template_config_class->perf_stat_stop( 'Postgres prepare for remote load' );

        $template_config_class->perf_stat_start('Postgres pg_putcopydata');

        while ( my $line = <$csv_file> ) {

            $dbh->pg_putcopydata( $line )
                || die( $dbh->errstr );

            $counter++;

            if ( $counter % 10000 == 0 ) {
                $self->log->info("Read [" . $self->comma_separated($counter) . "] lines ( not records; lines ) so far");
            }

        }

        $dbh->pg_putcopyend
            || die( "pg_putcopyend failed:\n" . $dbh->errstr );

    };

    $template_config_class->perf_stat_stop( 'Postgres pg_putcopydata' );

    my $error = $@;

    return {
        template_text       => $template_text
      , record_count        => ( $error ? 0 : $counter )
      , error               => $error
    };

}

sub does_table_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select table_name from information_schema.tables where table_schema = '" . $schema . "' and table_type like '%TABLE' and table_name = '" . $table . "'";

    return $sql;

}

sub does_schema_not_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select 'not exists' where not exists ( select nspname from pg_catalog.pg_namespace where nspname = '" . $schema . "' )";

    return $sql;

}

1;
