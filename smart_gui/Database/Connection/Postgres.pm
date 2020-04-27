package Database::Connection::Postgres;

use parent 'Database::Connection';

use strict;
use warnings;

use feature 'switch';

use Exporter qw ' import ';

use Time::HiRes;

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'unicodes';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'Postgres';

use Glib qw ' TRUE FALSE ';

sub new {
    
    my $self = shift->SUPER::new( @_ );
    
    if ( ! $self ) {
        return undef;
    }
    
    if ( $self->{connection} ) {
        $self->fetch_server_version();
    }
    
    return $self;
    
}

sub fetch_server_version {
    
    my $self = shift;
    
    my $version_aoh = $self->select( "select current_setting('server_version_num') as version_no" );
    $self->{server_version} = $$version_aoh[0]->{VERSION_NO};
    
    return $self->{server_version};
    
}

sub default_port {

    my $self = shift;

    return 5432;

}

sub connect_pre {
    
    my ( $self, $auth_hash, $options_hash ) = @_;
    
    # We *always* rebuild the connection string for Postgres, as we have to
    # include the database in the connection string
    $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
    
    my $dbi_options_hash = {
                               RaiseError        => 0
                             , AutoCommit        => 1
                           };
    
    ####################################################
    # TODO:    case handling ...
    # WARNING: legacy code / technical debt issues here!
    ####################################################
    
    # To maintain compatibility with Netezza ( which returns column names in UPPER CASE ),
    # we started using the below FetchHashKeyName hack to make other databases to the same.
    # This causes issues ( eg fetching primary key info from postgres, and also matching column names
    # returned by db-specific 'fetch column' SQL with column names returned by queries ).
    # We need to remove this and implement column name mangling explicitely and ONLY in the cases where we need it.
    
    # if ( ! $options_hash->{dont_force_case} ) {
    #     $dbi_options_hash->{FetchHashKeyName}  = 'NAME_uc';
    # }
    
    $options_hash->{dbi_options_hash} = $dbi_options_hash;
    
    return ( $auth_hash , $options_hash );
    
}

sub build_connection_string {
    
    my ( $self , $auth_hash , $string ) = @_;
    
    if ( ! $string ) { # subclasses can build their own connection string - in this case, don't re-assemble one here ...
    
        if ( ! $auth_hash->{Database} || $auth_hash->{Database} eq '+ Add New Database' ) {
            $auth_hash->{Database} = $self->default_database; # Postgres requires you to specify a database when connecting ...
        }
        
        if ( ! $auth_hash->{Port} ) {
            $auth_hash->{Port} = 5432;
        }
        
        no warnings 'uninitialized';
    
        $string =
            "dbi:Pg:dbname=" . $auth_hash->{Database}
          . ";host="         . $auth_hash->{Host}
          . ";port="         . $auth_hash->{Port};
        
        print "Postgres.pm assembled connection string: $string\n";
        
    }
    
    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub connection_label_map {
    
    return {
        Username        => "Username"
      , Password        => "Password"
      , Host_IP         => "Host / IP"
      , Port            => "Port"
      , Database        => "Default Database"
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };
    
}

sub default_database {
    
    my $self = shift;
    
    return 'postgres';
    
}

sub can_ddl_in_transaction {

    my $self = shift;

    return TRUE;

}

sub fetch_database_list {
    
    my $self = shift;
    
    my $sth = $self->prepare( "select datname from pg_database where datistemplate = false" )
        || return;
    
    $self->execute( $sth )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub _db_connection {
    
    my ( $self , $database ) = @_;

    print "_db_connection( $database ) called ...\n";

    my $connection = $self;
    
    if ( $database ne $self->{database} ) {

        print "_db_connection creating a new connection for this db ...\n";

        my %auth_hash = %{$self->{auth_hash}};
        
        $auth_hash{Database} = $database;
        
#        $connection = Database::Connection::Postgres->new(
#            $self->{globals}
#          , \%auth_hash
#        );
        
        $connection = Database::Connection::generate(
            $self->{globals}
          , \%auth_hash
        );
        
    }
    
    return $connection;
    
}

sub fetch_schema_list {
    
    my ( $self, $database ) = @_;
    
    my $connection = $self->_db_connection( $database );
    
    my $sth = $connection->prepare(
        "SELECT nspname FROM pg_catalog.pg_namespace where nspname not like 'pg_temp%' and nspname not like 'pg_toast%'" # do NOT use information_schema.schemata - it only shows schemas owned by the current user
    ) || return;
    
    $connection->execute( $sth )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;
    
    my $connection = $self->_db_connection( $database );
    
    my $sth = $connection->prepare(
        "select table_name from information_schema.tables where table_schema = ? and table_type like '%TABLE'" # TODO: split foreign and base tables, and any other types
    ) || return;
    
    $connection->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_view_list {
    
    my ( $self, $database, $schema ) = @_;
    
    my $connection = $self->_db_connection( $database );
    
    my $sth = $connection->prepare(
        "select table_name from information_schema.views where table_schema = ?"
    ) || return;
    
    $connection->execute( $sth, [ $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_materialized_view_list {

    my ( $self, $database, $schema ) = @_;

    if ( $self->{server_version} < 90300 ) {
        return ();
    }

    my $connection = $self->_db_connection( $database );

    my $sth = $connection->prepare(
        "select matviewname from pg_matviews where schemaname = ?"
    ) || return;

    $connection->execute( $sth, [ $schema ] )
        || return;

    my @return;

    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }

    return sort( @return );

}

sub fetch_view {
    
    my ( $self, $database, $schema, $view ) = @_;
    
    my $connection = $self->_db_connection( $database );
    
    my $sth = $connection->prepare(
        "select view_definition from information_schema.views\n"
      . "where  table_schema = ? and table_name = ?"
    ) || return;
    
    $connection->execute( $sth, [ $schema , $view ] )
        || return;
    
    my $row = $sth->fetchrow_arrayref;
    
    if ( $row ) {
        return 'create or replace view ' . $connection->db_schema_table_string( $database, $schema, $view ) . ' as ' . $$row[0];
    } else {
        return $$row[0];
    }
    
}

sub fetch_function_list {
    
    my ( $self, $database, $schema ) = @_;
    
    my $sth = $self->prepare(
        "select routines.routine_name from information_schema.routines where routines.specific_catalog = ? and routines.specific_schema=? group by routine_name"
    ) || return;
    
    $self->execute( $sth, [ $database, $schema ] )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_procedure_list {
    
    my ( $self, $database, $schema ) = @_;
    
    #######
    return ();
    #######
    
}

sub drop_db_string {
    
    my ( $self , $database , $options ) = @_;
    
    my $sql = "drop database " . '"' . $database . '"';
    
    return $sql;
    
}

sub db_schema_string {
    
    my ( $self, $database, $schema, $options ) = @_;
    
    # This method is basically only used when creating schemas, and Postgres doesn't appear
    # to support the syntax: create schema db.schema ... so we just return the schema
    
    if ( $options->{dont_quote} ) {
        return $schema;
    } else {
        return '"' . $schema . '"';
    }
    
}

sub db_schema_table_string {

    my ( $self, $database, $schema, $table, $options ) = @_;
    
    # $options contains:
    #{
    #    dont_quote      => 0
    #}
    
    if ( ! $schema ) {
        if ( $options->{dont_quote} ) {
            return $table;
        } else {
            return '"' . $table . '"';
        }
    } else {
        if ( $options->{dont_quote} ) {
            return $schema . '.' . $table;
        } else {
            return '"' . $schema . '"."' . $table . '"';
        }
    }

}

sub refresh_materialized_view_string {

    my ( $self, $database, $schema, $materialized_view ) = @_;

    return "refresh materialized view " . $schema . "." . $materialized_view;

}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    if ( ! $schema ) {
        $schema = 'public';
    }
    
    my $connection = $self->_db_connection( $database );
    
    use constant COLUMN_NUMBER  => 0;
    use constant COLUMN_NAME    => 1;
    use constant DATA_TYPE      => 2;
    use constant PRECISION      => 3;
    use constant NULLABLE       => 4;
    use constant COLUMN_DEFAULT => 5;
    
    my $sql = "select\n"
      . "    f.attnum AS number\n";
    
    if ( $options->{force_upper} ) {
        $sql .= "  , upper(f.attname) as COLUMN_NAME\n";
    } else {
        $sql .= "  , f.attname as COLUMN_NAME\n";
    }
    
    $sql .=
        "  , pg_catalog.format_type(f.atttypid,f.atttypmod) AS DATA_TYPE\n"
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
      . " and lower(n.nspname) = lower(?)\n" # NOTE: hack for case sensitive bullshit.
      . " and lower(c.relname) = lower(?)\n" # The DML export, amongst other things, needs this
      . " and f.attnum > 0\n"                # TODO: DML export no longer uses this
      . "order by\n"
      . "     number\n";
    
    print "\n\n$sql\n\n";
    
    my $sth = $connection->prepare(
        $sql
    ) || return;
    
    $connection->execute( $sth, [ $schema, $table ] )
        || return;
    
    my $column_info = $sth->fetchall_arrayref;
    
    my $return;
    
    # Split out the DATA_TYPE and PRECISION ...
    foreach my $column ( @{$column_info} ) {
        
        if ( $column->[ DATA_TYPE ] =~ /([\w\s]*)(\(.*\))/ ) {
            ( $column->[ DATA_TYPE ] , $column->[ PRECISION ] ) = ( $1 , $2 );
        }
        
        $return->{ $column->[ COLUMN_NAME ] } =
        {
              COLUMN_NAME     => $column->[ COLUMN_NAME ]
            , DATA_TYPE       => $column->[ DATA_TYPE ]
            , PRECISION       => $column->[ PRECISION ]
            , NULLABLE        => $column->[ NULLABLE ]
            , COLUMN_DEFAULT  => $column->[ COLUMN_DEFAULT ]
        };
        
    }
    
    return $return;
    
}

sub fetch_all_column_info {
    
    my ( $self, $database, $schema, $progress_bar, $options ) = @_; # $options currently unused
    
    # TODO: this code is ported from the above fetch_column_info()
    #       but has NOT BEEN TESTED yet
    
    if ( ! $schema ) {
        $schema = 'public';
    }
    
    my $connection = $self->_db_connection( $database );
    
    use constant TABLE_NAME     => 0;
    use constant COLUMN_NAME    => 1;
    use constant DATA_TYPE      => 2;
    use constant PRECISION      => 3;
    use constant NULLABLE       => 4;
    use constant COLUMN_DEFAULT => 5;
    
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!! NOTE !!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # If you change the query below, then change the constants above
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!! NOTE !!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    my $sql = "select\n"
            . "    c.relname as TABLE_NAME\n"
            . "  , f.attname as COLUMN_NAME\n"
            . "  , pg_catalog.format_type(f.atttypid,f.atttypmod) AS DATA_TYPE\n"
            . "  , NULL as PRECISION\n"                                              # Gets injected below
            . "  , case when f.attnotnull then 0 else 1 end as NULLABLE\n"
            . "from\n"
            . "             pg_attribute   f\n"
            . "    inner join pg_class     c\n"
            . "                               on c.oid = f.attrelid\n"
            . "    inner join pg_type      t\n"
            . "                               on t.oid = f.atttypid\n"
            . "    left join  pg_attrdef   d\n"
            . "                               on d.adrelid = c.oid\n"
            . "                              and d.adnum = f.attnum\n"
            . "    left join  pg_namespace n\n"
            . "                               on n.oid = c.relnamespace\n"
            . "where\n"
            . "     c.relkind = 'r'::char\n"
            . " and n.nspname = ?\n"
            . " and f.attnum > 0\n";
    
    print "\n\n$sql\n\n";
    
    my $sth = $connection->prepare(
        $sql
    ) || return;
    
    if ( $progress_bar ) {
        $progress_bar->set_text( "Fetching column info from database ..." );
        $self->kick_gtk;
    }
    
    $self->execute( $sth, [ $schema ] )
        || return;
    
    # It's fastest to pull everything at once ...
    my $all_column_info = $sth->fetchall_arrayref;
    
    my $no_of_records = @{$all_column_info};
    
    my $counter;
    my $return;
    
    # The top-level of this structure is a hash, with table names as the key.
    # Inside that, we have an array of column info records, sorted in the order we get
    # from the DB ( so columns appear in the order they're normally seen in )
    foreach my $column_info ( @{$all_column_info} ) {
        if ( $progress_bar ) {
            $counter ++;
            if ( $counter % 100 == 0 ) {
                $progress_bar->set_fraction( $counter / $no_of_records );
                $progress_bar->set_text( $column_info->[1] . " ..." );
                $self->kick_gtk;
            }
        }
        # Split out the DATA_TYPE and PRECISION ...
        # TODO: performance improvement ( probably minor I guess ): port from hash handling to array - see SQLServer classe
        if ( $column_info->[ DATA_TYPE ] =~ /([\w\s]*)(\(.*\))/ ) {
            ( $column_info->[ DATA_TYPE ] , $column_info->[ PRECISION ] ) = ( $1 , $2 );
        }
        
        push @{ $return->{ $column_info->[ TABLE_NAME ] } }
        , {
                COLUMN_NAME     => $column_info->[ COLUMN_NAME ]
              , DATA_TYPE       => $column_info->[ DATA_TYPE ]
              , PRECISION       => $column_info->[ PRECISION ]
              , NULLABLE        => $column_info->[ NULLABLE ]
              , COLUMN_DEFAULT  => $column_info->[ COLUMN_DEFAULT ]
              , TABLE_TYPE      => 'TABLE'
          };
    }
    
    if ( $progress_bar ) {
        $progress_bar->set_fraction( 0 );
        $progress_bar->set_text( "" );
        $self->kick_gtk;
    }
    
    return $return;
    
}

sub fetch_field_list {

    my ( $self, $database, $schema, $table, $options ) = @_;

    print "fetch_field_list( $database , $schema , $table ) called ...\n";

    my $connection = $self->_db_connection( $database );

    my $sth = $connection->prepare(
        "select * from " . $connection->db_schema_table_string( $database, $schema, $table ) . " where 0=1" );

    $connection->execute( $sth );

    my $fields;

    if ( $options->{dont_mangle_case} ) {
        $fields  =$sth->{NAME};
    } else {
        $fields = $sth->{NAME_uc};
    }

    $sth->finish();

    return $fields;

}

sub fetch_all_indexes {

    my ( $self, $database, $schema ) = @_;

    # NOTE: this query will currently only search in the DEFAULT database
    # ( there appears to be no way to include the database in the query )
    # To work around this ( and not switch the current DB underneath other code )
    # we CLONE the connection "use $database", and then run the query

    my $this_dbh = $self->clone;

    $this_dbh->do( "use $database" );

    my $sth;

    eval {

        $sth = $this_dbh->prepare(
            "select
    TABLE_NAME
  , INDEX_NAME
  , COLUMN_NAME
  , IS_UNIQUE
  , IS_PRIMARY
from
(
	select
		        c.relname                                     as TABLE_NAME
		      , i.relname                                     as INDEX_NAME
		      , f.attname                                     as COLUMN_NAME
		      , case when p.contype = 'u' then 1 else 0 end   as IS_UNIQUE
		      , case when p.contype = 'p' then 1 else 0 end   as IS_PRIMARY
		      , generate_subscripts(ix.indkey,1)              as INDEX_POSITION
              , unnest(ix.indkey)                             as UNNESTED_INDEX_KEY
		      , f.attnum                                      as ATTRIBUTE_NUMBER
	from
		        pg_attribute           f
		   join pg_class               c  on c.oid = f.attrelid
		   join pg_type                t  on t.oid = f.atttypid
	  left join pg_attrdef             d  on d.adrelid = c.oid and d.adnum = f.attnum
	  left join pg_namespace           n  on n.oid = c.relnamespace
	  left join pg_index               ix on f.attnum = any( ix.indkey ) and c.oid = f.attrelid and c.oid = ix.indrelid
	  left join pg_class               i  on ix.indexrelid = i.oid
	  left join pg_constraint          p  on ( p.conrelid = c.oid and f.attnum = any( p.conkey ) and p.conname = i.relname )
	  left join pg_class               g  on p.confrelid = g.oid
	where
    	    i.oid <> 0          -- it's an index
        and c.relkind = 'r'::char
        and n.nspname = ?
        and f.attnum > 0
    group by
        1 , 2 , 3 , 4 , 5 , 6 , 7 , 8
	order by
		c.relname
	  , f.attname
	  , INDEX_POSITION
) fetch_and_unnest -- need to do this in a subquery, because we can't have functions returning arrays in a where clause
where
    UNNESTED_INDEX_KEY = ATTRIBUTE_NUMBER
order by
    TABLE_NAME
  , INDEX_NAME
  , INDEX_POSITION"
        ) or die( $this_dbh->errstr );

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
    #        IS_PRIMARY      => 0 or 1
    #      , IS_UNIQUE       => 0 or 1
    #      , TABLE_NAME      => "TABLE_NAME"
    #      , COLUMNS         => [ "COL_1", "COL_2", etc ... ]
    #    }
    #};

    while ( my $row = $sth->fetchrow_hashref ) {

        $return->{ $row->{INDEX_NAME} }->{IS_PRIMARY} = $row->{IS_PRIMARY};
        $return->{ $row->{INDEX_NAME} }->{IS_UNIQUE}  = $row->{IS_UNIQUE};
        $return->{ $row->{INDEX_NAME} }->{TABLE_NAME} = $row->{TABLE_NAME};

        push @{ $return->{ $row->{INDEX_NAME} }->{COLUMNS} }
            , $row->{COLUMN_NAME};

    }

    $this_dbh->disconnect;

    return $return;

}

#sub fetch_all_foreign_key_info {
#
#    my ( $self, $database, $schema ) = @_;
#
#    # NOTE: this query will currently only search in the DEFAULT database
#    # ( there appears to be no way to include the database in the query )
#    # To work around this ( and not switch the current DB underneath other code )
#    # we CLONE the connection "use $database", and then run the query
#
#    my $this_dbh = $self->clone;
#
#    $this_dbh->do( "use $database" );
#
#    my $sth;
#
#    eval {
#
#        $sth = $this_dbh->prepare(
#            "select\n"
#          . "            upper(OBJECTS.name)             as FOREIGN_KEY_NAME\n"
#          . "          , upper(PRIMARY_SCHEMA.name)      as PRIMARY_SCHEMA\n"
#          . "          , upper(PRIMARY_TABLE.name)       as PRIMARY_TABLE\n"
#          . "          , upper(PRIMARY_COLUMNS.name)     as PRIMARY_COLUMN\n"
#          . "          , upper(REFERENCED_SCHEMA.name)   as REFERENCED_SCHEMA\n"
#          . "          , upper(REFERENCED_TABLE.name)    as REFERENCED_TABLE\n"
#          . "          , upper(REFERENCED_COLUMNS.name)  as REFERENCED_COLUMN\n"
#          . "from\n"
#          . "            sys.foreign_key_columns     FOREIGN_KEY_COLUMNS\n"
#          . "inner join  sys.objects                 OBJECTS\n"
#          . "                                                                on OBJECTS.object_id            = FOREIGN_KEY_COLUMNS.constraint_object_id\n"
#          . "inner join  sys.tables                  PRIMARY_TABLE\n"
#          . "                                                                on PRIMARY_TABLE.object_id      = FOREIGN_KEY_COLUMNS.parent_object_id\n"
#          . "inner join  sys.schemas                 PRIMARY_SCHEMA\n"
#          . "                                                                on PRIMARY_TABLE.schema_id      = PRIMARY_SCHEMA.schema_id\n"
#          . "inner join  sys.columns                 PRIMARY_COLUMNS\n"
#          . "                                                                on PRIMARY_COLUMNS.column_id    = parent_column_id\n"
#          . "                                                               and PRIMARY_COLUMNS.object_id    = PRIMARY_TABLE.object_id\n"
#          . "inner join  sys.tables                  REFERENCED_TABLE\n"
#          . "                                                                on REFERENCED_TABLE.object_id   = FOREIGN_KEY_COLUMNS.referenced_object_id\n"
#          . "inner join  sys.schemas                 REFERENCED_SCHEMA\n"
#          . "                                                                on REFERENCED_TABLE.schema_id   = REFERENCED_SCHEMA.schema_id\n"
#          . "inner join  sys.columns                 REFERENCED_COLUMNS\n"
#          . "                                                                on REFERENCED_COLUMNS.column_id = referenced_column_id\n"
#          . "                                                               and REFERENCED_COLUMNS.object_id = REFERENCED_TABLE.object_id\n"
#          . "where\n"
#          . "            PRIMARY_SCHEMA.name  = ?"
#        ) or die( $this_dbh->errstr );
#
#    };
#
#    my $err = $@;
#
#    if ( $err ) {
#
#        $self->dialog(
#            {
#                title       => "Error fetching foreign key info"
#              , type        => "error"
#              , text        => $err
#            }
#        );
#
#        return;
#
#    }
#
#    print "\n" . $sth->{Statement} . "\n";
#
#    eval {
#
#        $sth->execute( $schema )
#            or die( $sth->errstr );
#
#    };
#
#    $err = $@;
#
#    if ( $err ) {
#
#        $self->dialog(
#            {
#                title       => "Error fetching foreign key info"
#              , type        => "error"
#              , text        => $err
#            }
#        );
#
#        return;
#
#    }
#
#    my $return;
#
#    while ( my $row = $sth->fetchrow_hashref ) {
#
#        $return->{ $row->{FOREIGN_KEY_NAME} }->{PRIMARY_SCHEMA}    = $row->{PRIMARY_SCHEMA};
#        $return->{ $row->{FOREIGN_KEY_NAME} }->{PRIMARY_TABLE}     = $row->{PRIMARY_TABLE};
#        $return->{ $row->{FOREIGN_KEY_NAME} }->{REFERENCED_SCHEMA} = $row->{REFERENCED_SCHEMA};
#        $return->{ $row->{FOREIGN_KEY_NAME} }->{REFERENCED_TABLE}  = $row->{REFERENCED_TABLE};
#
#        $return->{ $row->{FOREIGN_KEY_NAME} }->{PRIMARY_COLUMN} = $row->{PRIMARY_COLUMN};
#
#        push @{ $return->{ $row->{FOREIGN_KEY_NAME} }->{RELATIONSHIP_COLUMNS} }
#            , {
#                PRIMARY_COLUMN      => $row->{PRIMARY_COLUMN}
#              , REFERENCED_COLUMN   => $row->{PRIMARY_COLUMN}
#            };
#
#    }
#
#    $this_dbh->disconnect;
#
#    return $return;
#
#}

sub _ddl_mangler_NUMBER {
    
    my ( $self, $type, $precision_scale ) = @_;
    
    my $return_type;
    
    if ( $precision_scale =~ /\(([\d]+),*([\d]*)\)/ ) {
        
        my ( $precision , $scale ) = ( $1 , $2 );
        
        if ( ! $scale ) {
            
            # If there is no scale, then we convert to an INT type, which is more efficient
            # and in Netezza also gets zone maps
            
            $precision_scale = undef;
            
            if (      $precision < 3) {
                $return_type = 'BYTEINT';
            } elsif ( $precision < 5 ) {
                $return_type = 'SMALLINT';
            } elsif ( $precision < 10 ) {
                $return_type = 'INTEGER';
            } else {
                $return_type = 'BIGINT';
            }
            
        } else {
            
            $return_type = 'NUMERIC';
            
        }
        
    } else {
        
        $return_type            = 'NUMERIC';
        
    }
    
    return {
        type            => $return_type
      , precision_scale => $precision_scale
    };
    
}

sub _model_to_fk_rel_ddl {

    my ( $self, $mem_dbh, $object_recordset ) = @_;
    
    # Note: we currently ignore the $database and $schema as the migration wizard ( which populates
    #       the database in $mem_dbh ) ONLY populates for a single given database/schema. But in the
    #       future, we might want to pull in a list of databases and schemas into the model, and so
    #       we'll *then* have to filter on db/schema
    
    my $fk_structure = $self->_model_to_fk_structure( $mem_dbh, $object_recordset->{primary_database}, $object_recordset->{primary_schema}, $object_recordset->{relationship_name} );
    
    my $primary_db_schema_table = $object_recordset->{primary_database} . "." . $object_recordset->{primary_schema} . "." . $object_recordset->{primary_table};
    my $foreign_db_schema_table = $object_recordset->{foreign_database} . "." . $object_recordset->{foreign_schema} . "." . $object_recordset->{foreign_table};


    my $sql = "alter table    $primary_db_schema_table\n"
            # NP This key $object_recordset->{constraint_name} apears to be called relationship_name now, so was no name for the constraint DDL
            #. "add constraint " . $object_recordset->{constraint_name} . "\n"
            . "add constraint " . $object_recordset->{relationship_name} . "\n"
            . "foreign key    ( " . join( " , ", @{$fk_structure->{PRIMARY_COLUMNS}} ) . " )\n"
            . "references     $foreign_db_schema_table ( " . join( " , ", @{$fk_structure->{FOREIGN_COLUMNS}} ) . " )\n"; # NP TODO There's a "Use of uninitialized value in concatenation"
    
    return {
        ddl         => $sql
      , warnings    => undef
    };
    
}

sub _model_to_index_ddl {
    
    my ( $self, $mem_dbh, $object_recordset ) = @_;
    
    my $index_structure = $mem_dbh->_model_to_index_structure( $object_recordset->{database_name}, $object_recordset->{schema_name}, $object_recordset->{table_name}, $object_recordset->{index_name} );
    
    my $primary_db_schema_table = $object_recordset->{database_name} . "." . $object_recordset->{schema_name} . "." . $object_recordset->{table_name};
    
    # NP Just adding unique ones for a short time
    # TODO: This is INCOMPLETE !!!
    my $sql;
    
    if ( $object_recordset->{is_unique} ) {
        
        my $unique_flag = $object_recordset->{is_unique} ? "UNIQUE" : "";
        
        $sql = "alter table    $primary_db_schema_table\n"
                . "add constraint " . $object_recordset->{index_name} . " $unique_flag\n";
        
        my $cols = [];
        
        map  {push @{ $cols }, $_->{column_name} } @{$index_structure}; 
        
        my $col_str = join ", ", @{ $cols };
        
        $sql .= "( $col_str )\n";
        
    }
    
    return {
        ddl         => $sql
      , warnings    => undef
    };
    
}

sub _model_to_sequence_ddl {

    my ( $self, $mem_dbh, $object_recordset ) = @_;

    my $sql;

    return {
        ddl         => $sql
      , warnings    => undef
    };

}

sub _model_to_primary_key_ddl {
    
    my ( $self, $mem_dbh, $object_recordset ) = @_;
    
    my $pk_structure = $mem_dbh->_model_to_primary_key_structure( $object_recordset->{database_name},  $object_recordset->{schema_name} , $object_recordset->{table_name} );
    
    my $primary_db_schema_table = $object_recordset->{database_name} . "." . $object_recordset->{schema_name} . "." . $object_recordset->{table_name};
    
    # ALTER TABLE thingy ADD CONSTRAINT PK_COL1 PRIMARY KEY (col1);
    
    my $sql = "alter table    $primary_db_schema_table\n"
            . "add constraint " . $object_recordset->{index_name} . "\n";
    
    my $cols = [];
    
    map { push @{ $cols }, $_->{column_name} } @{$pk_structure}; 
    
    #. "primary key    ( " . join( " , ", @{$pk_structure->{PRIMARY_COLUMNS}} ) . " )\n";
    
    my $col_str = join " , ", @{ $cols };
    
    $sql .= "primary key ( $col_str )\n";
    
    return {
        ddl         => $sql
      , warnings    => undef
    };
        
}

# sub _model_to_view_ddl {
#
#     my ( $self, $control_dbh, $object_recordset ) = @_;
#
#     my $view_definition = $object_recordset->{view_definition};
#
#     # Convert square brackets ( ie [] ) to quotes, *and* force text inside the square brackets to upper-case
#
#     $view_definition =~ s/\[([\w]*)\]/'"' . uc($1) . '"'/ge;
#
#     # NP Now Look for nolocks directive in DDL
#
#     if ( $view_definition =~ /nolock/im ) {
#
#         #warn "View $view_name is using 'with (nolock)'... So just checking for other options to try and preserve them if need be";
#
#         # NP Alrighty, let's have a closer look... if it's just nolock in the with clause get rid of it completely, including the 'with (.*)'..
#         # If not, try preserving the other options (I'm not sure what other options or delimiter is available here, this is used at table level a lot - which would be harder to parse out)
#
#         my $exp = 'with\s*?\((.*?)\s*?nolock(.*?\))';
#
#         if ( $view_definition =~ /$exp/im ) {
#
#             # NP This is fairly fragile... :/
#             if ( $1 && $2 =~ /\)/ ) {
#
#                 # we need to preserve the other stuff
#                 warn "Hang on a sec.. I don't know what to do about option(s) $1 yet..";
#
#             } else {
#
#                 #$view_definition =~ s/with\s*?\(\s*?nolock.*?\)//img;
#                 $view_definition =~ s/$exp//img;
#
#             }
#
#         }
#
#     }
#
#     my $ddl_manglers = $control_dbh->select(
#         "select * from object_manglers where object_type = 'view' and enabled = 1"
#     );
#
#     foreach my $ddl_mangler ( @{$ddl_manglers} ) {
#
#         my $regex_search  = $ddl_mangler->{regex_search};
#         my $regex_replace = $ddl_mangler->{regex_replace};
#
#         $view_definition  =~ s/$regex_search/$regex_replace/gi;
#
#     }
#
#     return {
#         ddl         => $view_definition
#       , warnings    => undef
#     };
#
# }

sub insert_step_before {

    my ( $self, $processing_group, $sequence_order ) = @_;

    $self->begin_work;

    eval {

        # We have unique keys defined on ( PROCESSING_GROUP , SEQUENCE_ORDER ) ... so for DBs that
        # enforce it, we make use of negative numbers to avoid temporarily breaking uniqueness rules,
        # then use abs() to make them positive again

        # CONFIG:
        $self->do(
            "update CONFIG set\n"
          . "    SEQUENCE_ORDER        = case when SEQUENCE_ORDER >= ?        then -( SEQUENCE_ORDER + 1 ) else SEQUENCE_ORDER end\n"
          . "  , PARENT_SEQUENCE_ORDER = case when PARENT_SEQUENCE_ORDER >= ? then -( PARENT_SEQUENCE_ORDER + 1 ) else PARENT_SEQUENCE_ORDER end\n"
          . "where PROCESSING_GROUP_NAME = ?"
          , [ $sequence_order , $sequence_order , $processing_group ]
        ) || die( "CONFIG update failed" );

        $self->do(
            "update CONFIG set\n"
          . "    SEQUENCE_ORDER        = abs( SEQUENCE_ORDER ) \n"
          . "  , PARENT_SEQUENCE_ORDER = abs( PARENT_SEQUENCE_ORDER )"
          . "where PROCESSING_GROUP_NAME = ?"
          , [ $processing_group ]
        ) || die( "CONFIG update failed" );

        # PARAM_VALUE:
        $self->do(
            "update PARAM_VALUE set\n"
          . "    SEQUENCE_ORDER = -( SEQUENCE_ORDER + 1 )\n"
          . "where PROCESSING_GROUP_NAME = ? and SEQUENCE_ORDER >= ?"
          , [ $processing_group , $sequence_order ]
        ) || die( "CONFIG update failed" );

        $self->do(
            "update PARAM_VALUE set\n"
          . "SEQUENCE_ORDER = abs( SEQUENCE_ORDER )\n"
          . "where PROCESSING_GROUP_NAME = ?"
          , [ $processing_group ]
        ) || die( "PARAM_VALUE update failed" );

        $self->do( "insert into CONFIG ( PROCESSING_GROUP_NAME , SEQUENCE_ORDER , TEMPLATE_NAME , CONNECTION_NAME ) values ( ? , ? , ? , '' )"
          , [ $processing_group , $sequence_order , 'GENERIC_SQL' ]
        ) || die( "CONFIG insert failed" );

    };

    if ( $@ ) {

        $self->dialog(
            {
                title   => "Insert before failed"
              , type    => "error"
              , text    => $@ . "\nRolling back changes ..."
            }
        );

        $self->rollback;

    } else {

        $self->commit;

    }

}

sub insert_step_after {

    my ( $self, $processing_group, $sequence_order ) = @_;

    $self->insert_step_before( $processing_group , $sequence_order + 1 );

}

sub move_step_down {
    
    my ( $self, $processing_group, $sequence_order ) = @_;
    
    $self->begin_work;
    
    eval {
        
        # We have unique keys defined on ( PROCESSING_GROUP , SEQUENCE_ORDER ) ... so for DBs that
        # enforce it, we make use of negative numbers to avoid temporarily breaking uniqueness rules,
        # then use abs() to make them positive again
        
        # CONFIG:
        $self->do(
            "update CONFIG set\n"
          . "    SEQUENCE_ORDER = case when SEQUENCE_ORDER = ? then -( SEQUENCE_ORDER + 1 ) else -( ? )::INT end\n"
          . "where PROCESSING_GROUP_NAME = ? and SEQUENCE_ORDER between ? and ? + 1"
          , [ $sequence_order, $sequence_order, $processing_group, $sequence_order, $sequence_order ]
        ) || die( "CONFIG update failed" );
        
        $self->do(
            "update CONFIG set\n"
          . "SEQUENCE_ORDER = abs(SEQUENCE_ORDER) \n"
          . "where PROCESSING_GROUP_NAME = ?"
          , [ $processing_group ]
        ) || die( "CONFIG update failed" );
        
        # PARAM_VALUE:
        $self->do(
            "update PARAM_VALUE set\n"
          . "SEQUENCE_ORDER = case when SEQUENCE_ORDER = ? then -( SEQUENCE_ORDER + 1 ) else -( ? )::INT end\n"
          . "where PROCESSING_GROUP_NAME = ? and SEQUENCE_ORDER between ? and ? + 1"
          , [ $sequence_order, $sequence_order, $processing_group, $sequence_order, $sequence_order ]
        ) || die( "CONFIG update failed" );
        
        $self->do(
            "update PARAM_VALUE set\n"
          . "SEQUENCE_ORDER = abs(SEQUENCE_ORDER)\n"
          . "where PROCESSING_GROUP_NAME = ?"
          , [ $processing_group ]
        ) || die( "PARAM_VALUE update failed" );
        
    };
    
    if ( $@ ) {
        
        $self->dialog(
            {
                title   => "Move failed"
              , type    => "error"
              , text    => $@ . "\nRolling back changes ..." 
            }
        );
        
        $self->rollback;
        
    } else {
        
        $self->commit;
        
    }
    
}

sub move_step_up {
    
    my ( $self, $processing_group, $sequence_order ) = @_;
    
    $self->begin_work;
    
    eval {
        
        # We have unique keys defined on ( PROCESSING_GROUP , SEQUENCE_ORDER ) ... so for DBs that
        # enforce it, we make use of negative numbers to avoid temporarily breaking uniqueness rules,
        # then use abs() to make them positive again
        
        # CONFIG:
        $self->do(
            "update CONFIG set\n"
          . "SEQUENCE_ORDER = case when SEQUENCE_ORDER = ? then -( SEQUENCE_ORDER - 1 ) else -( ? )::INT end\n"
          . "where PROCESSING_GROUP_NAME = ? and SEQUENCE_ORDER between ? - 1 and ?"
          , [ $sequence_order, $sequence_order, $processing_group, $sequence_order, $sequence_order ]
        ) || die( "CONFIG update failed" );
        
        $self->do(
            "update CONFIG set\n"
          . "SEQUENCE_ORDER = abs(SEQUENCE_ORDER) \n"
          . "where PROCESSING_GROUP_NAME = ?"
          , [ $processing_group ]
        ) || die( "CONFIG update failed" );
        
        # PARAM_VALUE:
        $self->do(
            "update PARAM_VALUE set\n"
          . "SEQUENCE_ORDER = case when SEQUENCE_ORDER = ? then -( SEQUENCE_ORDER - 1 ) else -( ? )::INT end\n"
          . "where PROCESSING_GROUP_NAME = ? and SEQUENCE_ORDER between ? - 1 and ?"
          , [ $sequence_order, $sequence_order, $processing_group, $sequence_order, $sequence_order ]
        ) || die( "PARAM_VALUE update failed" );
        
        $self->do(
            "update PARAM_VALUE set\n"
          . "SEQUENCE_ORDER = abs(SEQUENCE_ORDER)\n"
          . "where PROCESSING_GROUP_NAME = ?"
          , [ $processing_group ]
        ) || die( "PARAM_VALUE update failed" );
        
    };
    
    if ( $@ ) {
        
        $self->dialog(
            {
                title   => "Move failed"
              , type    => "error"
              , text    => $@ . "\nRolling back changes ..." 
            }
        );
        
        $self->rollback;
        
    } else {
        
        $self->commit;
        
    }
    
}

sub generate_current_activity_query {
    
    my $self = shift;
    
    #return "select\n"
    #     . "    pid, state, query\n"
    #     . "from\n"
    #     . "    pg_stat_activity";
    
    return "select\n"
         . "    usename     as username\n"
         . "  , datname     as db\n"
         . "  , client_addr as host\n"
         . "  , pid         as id\n"
         . "  , state       as state\n"
         . "  , query       as query\n"
         . "from\n"
         . "    pg_stat_activity";
         
}

sub generate_query_cancel_sql {
    
    my ( $self, $pid ) = @_;
    
    return "select * from pg_cancel_backend( $pid )";
    
}

sub generate_session_kill_sql {

    my ( $self , $pid ) = @_;

    return "select * from pg_terminate_backend( $pid )";

}

###########################################################
# The following methods serve the window::data_loader class
###########################################################

#sub sql_to_csv {
#    
#    my ( $self, $options ) = @_;
#    
#    # options looks like:
##    {
##        file_path       => $file_path
##      , delimiter       => $delimiter
##      , quote_char      => $quote_char
##      , encoding        => $encoding
##      , sql             => $sql
##    }
#    
#    # NOTE: it appears to be IMPOSSIBLE to do an accelerated data export from a remote client
#    #       ( at least via an API ).
#    # We'd have to shell out to psql ...
#    # Another option is to just use our generic sql_to_csv, which works fine
#    
#}

sub generate_db_load_command {
    
    my ( $self, $options ) = @_;
    
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
                   . "\nwith\n(\n   format 'csv'";
    
    if ( $options->{skip_rows} ) {
        $copy_command .= ", header";
    }
    
    if ( $options->{delimiter} ) {
        $copy_command .= ", delimiter E'" . $options->{delimiter} . "'";
    }
    
    if ( $options->{encoding} ) {
        $copy_command .= ", encoding'" . $options->{encoding} . "'";
    }
    
    if ( $options->{escape_char} ) {
        $copy_command .= ", escape '" . $options->{escape_char} . "'";
    }
    
    if ( $options->{quote_char} ) {
        $copy_command .= ", quote '" . $options->{quote_char} . "'";
    }
    
    $copy_command .= "\n);";
    
    return $copy_command;
    
}

sub load_csv {
    
    # This method loads data, and is called from window::data_loader
    
    my ( $self, $options ) = @_;
    
    # options:
    #{
    #    mem_dbh             => $mem_dbh                - not in use for Postgres
    #  , target_db           => $target_db              - not in use for Postgres
    #  , target_schema       => $target_schema          - not in use for Postgres
    #  , target_table        => $target_table           - not in use for Postgres
    #  , table_definition    => $table_definition       - not in use for Postgres
    #  , copy_command        => $copy_command
    #  , remote_client       => $remote_client
    #  , file_path           => $file_path
    #  , progress_bar        => $progress_bar
    #  , suppress_dialog     => $suppress_dialog
    #}
    
    print( "\n$options->{copy_command}\n" );
    
    my $start_ts = Time::HiRes::gettimeofday;
    
    my $records;
    my $csv_file;
    
    if ( $options->{remote_client} ) {
        
        eval {
            open $csv_file, "<", $options->{file_path}
                || die( $! );
        };
        
        my $err = $@;
        
        if ( $err ) {
            
            $self->dialog(
                {
                    title       => "Failed to open local file for reading!"
                  , type        => "error"
                  , text        => $err
                }
            );
            
            return FALSE;
            
        }
        
        $self->do( $options->{copy_command} ) || return;
        
        my $counter;
        
        while ( my $line = <$csv_file> ) {
            $self->pg_putcopydata( $line );
            $counter ++;
            if ( $counter % 10000 == 0 ) {
                if ( $options->{progress_bar} ) {
                    my $formatted_counter = $counter;
                    $formatted_counter =~ s/(\d)(?=(\d{3})+(\D|$))/$1\,/g;
                    $options->{progress_bar}->set_text( $formatted_counter );
                    $options->{progress_bar}->pulse;
                    $self->kick_gtk;
                }
            }
        }
        
        eval {
            $self->pg_putcopyend
                || die( "Oh no!" );
        };
        
        $err = $self->errstr;
        
        if ( $err ) {
            
            $self->dialog(
                {
                    title       => "Failed to load local file!"
                  , type        => "error"
                  , text        => $err
                }
            );
            
            return FALSE;
            
        }
        
        $records = " UNKNOWN - $counter lines ";
        
    } else {
        
        $records = $self->do( $options->{copy_command} ) || return;
        
    }
    
    my $end_ts   = Time::HiRes::gettimeofday;
    
    if ( ! $options->{suppress_dialog} ) {
        
        $self->dialog(
            {
                title       => "Import Complete"
              , type        => "info"
              , text        => "[$records] records inserted in " . ( $end_ts - $start_ts ) . " seconds\n"
                             . "You can use the 'browser' window ( menu entry from the main window ) to view the data ..."
            }
        );
        
    }
    
    return TRUE;
    
}

sub generate_harvest_job {
    
    my ( $self, $options ) = @_;
    
    my $group_definition = {
        PROCESSING_GROUP_NAME           => $options->{processing_group}
      , PROCESSING_GROUP_DESCRIPTION    => "Auto-generated Harvest job"
      , TAGS                            => "harvest"
    };
    
    my $file_path                       =  '#ENV_HARVEST_PATH#';
    
    # Datacop. This step splits a CSV into 'good' and 'bad' files, as Postgres
    # isn't able to do this itself
    
    $self->{globals}->{windows}->{'window::main'}->autogen(
        {
            group   => $group_definition                                        # passing a group definition will create a new processing group
          , config  => {
                PROCESSING_GROUP_NAME       => $options->{processing_group}
              , TEMPLATE_NAME               => 'DATACOP'
              , CONNECTION_NAME             => $options->{target_connection}
              , SOURCE_DB_NAME              => ''
              , SOURCE_SCHEMA_NAME          => ''
              , SOURCE_TABLE_NAME           => ''
              , TARGET_DB_NAME              => $options->{target_database}
              , TARGET_SCHEMA_NAME          => $options->{staging_schema}
              , TARGET_TABLE_NAME           => $options->{target_table}
            }
          , param_value => {
                '#P_FILE_NAME#'             => $file_path
              , '#P_ESCAPE_CHAR#'           => $options->{escape_character}
              , '#P_INCLUDES_HEADERS#'      => $options->{includes_headers} # TODO #P_INCLUDES_HEADRS# vs #P_INCLUDES_HEADER#, below
          }
        }
    );
    
    # Truncate staging table
    
    $self->{globals}->{windows}->{'window::main'}->autogen(
        {
            config  => {
                PROCESSING_GROUP_NAME       => $options->{processing_group}
              , TEMPLATE_NAME               => 'TRUNCATE_TABLE'
              , CONNECTION_NAME             => $options->{target_connection}
              , SOURCE_DB_NAME              => ''
              , SOURCE_SCHEMA_NAME          => ''
              , SOURCE_TABLE_NAME           => ''
              , TARGET_DB_NAME              => $options->{target_database}
              , TARGET_SCHEMA_NAME          => $options->{staging_schema}
              , TARGET_TABLE_NAME           => $options->{target_table}
            }
          , param_value => { }
        }
    );
    
    # Postgres Load. This loads the CSV into the staging table ( ie in the staging schema )
    
    $self->{globals}->{windows}->{'window::main'}->autogen(
        {
            config  => {
                PROCESSING_GROUP_NAME       => $options->{processing_group}
              , TEMPLATE_NAME               => 'POSTGRES_9_X_LOAD'
              , CONNECTION_NAME             => $options->{target_connection}
              , SOURCE_DB_NAME              => ''
              , SOURCE_SCHEMA_NAME          => ''
              , SOURCE_TABLE_NAME           => ''
              , TARGET_DB_NAME              => $options->{target_database}
              , TARGET_SCHEMA_NAME          => $options->{staging_schema}
              , TARGET_TABLE_NAME           => $options->{target_table}
            }
          , param_value => {
                '#P_DELIMITER#'             => $options->{delimiter}
              , '#P_ENCODING#'              => $options->{encoding}
              , '#P_ESCAPE_CHAR#'           => $options->{escape_character}
              , '#P_FILE_NAME#'             => $file_path . ".good"
              , '#P_INCLUDES_HEADER#'       => $options->{includes_headers}
          }
        }
    );
    
    # Insert. This copies from the table in the staging schema into the target schema
    
    $self->{globals}->{windows}->{'window::main'}->autogen(
        {
            config => {
                PROCESSING_GROUP_NAME           => $options->{processing_group}
              , TEMPLATE_NAME                   => 'INSERT_SRC_TGT_DB_SCHEMA_TABLE'
              , CONNECTION_NAME                 => $options->{target_connection}
              , SOURCE_DB_NAME                  => $options->{target_database}
              , SOURCE_SCHEMA_NAME              => $options->{staging_schema}
              , SOURCE_TABLE_NAME               => $options->{target_table}
              , TARGET_DB_NAME                  => $options->{target_database}
              , TARGET_SCHEMA_NAME              => $options->{target_schema}
              , TARGET_TABLE_NAME               => $options->{target_table}
            }
          , param_value => { }
        }
    );
    
}

1;
