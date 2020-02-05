package Database::Connection;

use 5.20.0;

use parent 'window';

use strict;
use warnings;

use feature 'switch';

use IO::Socket::INET;
use Net::OpenSSH;
use Text::CSV;
use Glib qw | TRUE FALSE |;

use Carp;

use vars qw ' $AUTOLOAD ';

use constant    PERL_ZERO_RECORDS_INSERTED      => '0E0';

# These constants are the max allowed value for the INT types. Note that these are appropriate for POSTGRES.
# They are currently used in the DataCop class.
# Subclasses should override these if necessary.

use constant    BYTEINT_MAX                         => 0;
use constant    SMALLINT_MAX                        => 327688;
use constant    INTEGER_MAX                         => 2147483648;
use constant    BIGINT_MAX                          => "9223372036854775808";

# These constants are Perl's integer typecodes for SQL column types ...
use constant    SQL_GUID                            => -11;
use constant    SQL_WLONGVARCHAR                    => -10;
use constant    SQL_WVARCHAR                        =>  -9;
use constant    SQL_WCHAR                           =>  -8;
use constant    SQL_BIGINT                          =>  -5;
use constant    SQL_BIT                             =>  -7;
use constant    SQL_TINYINT                         =>  -6;
use constant    SQL_LONGVARBINARY                   =>  -4;
use constant    SQL_VARBINARY                       =>  -3;
use constant    SQL_BINARY                          =>  -2;
use constant    SQL_LONGVARCHAR                     =>  -1;
use constant    SQL_UNKNOWN_TYPE                    =>   0;
use constant    SQL_ALL_TYPES                       =>   0;
use constant    SQL_CHAR                            =>   1;
use constant    SQL_NUMERIC                         =>   2;
use constant    SQL_DECIMAL                         =>   3;
use constant    SQL_INTEGER                         =>   4;
use constant    SQL_SMALLINT                        =>   5;
use constant    SQL_FLOAT                           =>   6;
use constant    SQL_REAL                            =>   7;
use constant    SQL_DOUBLE                          =>   8;
use constant    SQL_DATETIME                        =>   9;
use constant    SQL_DATE                            =>   9;
use constant    SQL_INTERVAL                        =>  10;
use constant    SQL_TIME                            =>  10;
use constant    SQL_TIMESTAMP                       =>  11;
use constant    SQL_VARCHAR                         =>  12;
use constant    SQL_BOOLEAN                         =>  16;
use constant    SQL_UDT                             =>  17;
use constant    SQL_UDT_LOCATOR                     =>  18;
use constant    SQL_ROW                             =>  19;
use constant    SQL_REF                             =>  20;
use constant    SQL_BLOB                            =>  30;
use constant    SQL_BLOB_LOCATOR                    =>  31;
use constant    SQL_CLOB                            =>  40;
use constant    SQL_CLOB_LOCATOR                    =>  41;
use constant    SQL_ARRAY                           =>  50;
use constant    SQL_ARRAY_LOCATOR                   =>  51;
use constant    SQL_MULTISET                        =>  55;
use constant    SQL_MULTISET_LOCATOR                =>  56;
use constant    SQL_TYPE_DATE                       =>  91;
use constant    SQL_TYPE_TIME                       =>  92;
use constant    SQL_TYPE_TIMESTAMP                  =>  93;
use constant    SQL_TYPE_TIME_WITH_TIMEZONE         =>  94;
use constant    SQL_TYPE_TIMESTAMP_WITH_TIMEZONE    =>  95;
use constant    SQL_INTERVAL_YEAR                   => 101;
use constant    SQL_INTERVAL_MONTH                  => 102;
use constant    SQL_INTERVAL_DAY                    => 103;
use constant    SQL_INTERVAL_HOUR                   => 104;
use constant    SQL_INTERVAL_MINUTE                 => 105;
use constant    SQL_INTERVAL_SECOND                 => 106;
use constant    SQL_INTERVAL_YEAR_TO_MONTH          => 107;
use constant    SQL_INTERVAL_DAY_TO_HOUR            => 108;
use constant    SQL_INTERVAL_DAY_TO_MINUTE          => 109;
use constant    SQL_INTERVAL_DAY_TO_SECOND          => 110;
use constant    SQL_INTERVAL_HOUR_TO_MINUTE         => 111;
use constant    SQL_INTERVAL_HOUR_TO_SECOND         => 112;
use constant    SQL_INTERVAL_MINUTE_TO_SECOND       => 113;

sub AUTOLOAD {
    
    my ( $self, @args ) = @_;
    
    # Perl is cool ;) What we're doing here is catching any method calls that we don't implement ourself,
    # and passing them through to the connection object
    
    my $method = $AUTOLOAD;
    
    # strip out our own class, which will leave the method name we should pass through to DBI ...
    my $class = ref $self;
    $method =~ s/$class\:\://;
    
    if ( $class && exists $self->{connection} ) { # otherwise we get errors during global destruction
        return $self->{connection}->$method( @args );
    }
    
}

sub get_info {

    my ( $self , $type ) = @_;

    return $self->{connection}->get_info( $type );

}

sub generate {
    
    my ( $globals, $auth_hash, $dont_connect, $config_manager_type, $progress_bar, $options_hash , $dont_cache ) = @_;
    
    # This STATIC FUNCTION ( not a method ) will determine which subclass of
    # Database::Connection we need, and construct an object of that type
    
    my $connection_class        = $auth_hash->{DatabaseType};
    
    my $object_class            = 'Database::Connection::' . $connection_class;

    my $connection_key          = $auth_hash->{ConnectionName} . ":" . ( defined $auth_hash->{Database} ? $auth_hash->{Database} : $auth_hash->{Host} ); # For SQLite, the key we need to use is 'Host' ( path to file )

    if (
            defined $auth_hash->{ConnectionName}    # We can get called with no connection name, ie we're *just* building a connection class without connecting. In that case, skip the connection manager
         && ! $dont_cache
    ) {
        # Check in user connections pool to see if we already have one of these ...
        if ( exists $globals->{user_connections}->{ $connection_key } ) {
            return $globals->{user_connections}->{ $connection_key };
        }
    }

    # Convert path name into relative path
    my $class_relative_path = $object_class;
    $class_relative_path =~ s/:/\//g;
    
    # We also need the '.pm' at the end ...
    $class_relative_path .= '.pm';
    
    my @all_paths = $globals->{local_db}->all_gui_paths;
    
    foreach my $include_path ( @all_paths ) {
        if ( -e $include_path . "/" . $class_relative_path ) {
            print "Loading Perl resource [" . $include_path . "/" . $class_relative_path . "] for connection class [$object_class]\n";
            eval {
                require $include_path . "/" . $class_relative_path;
            };
            my $error = $@;
            if ( $error ) {
                warn( "$error" );
                dialog(
                    undef
                  , {
                        title       => "Compilation error!"
                      , type        => "error"
                      , text        => $error
                    }
                );
            }
        }
    }
    
    my $connection_object       = $object_class->new(
        $globals
      , $auth_hash
      , $dont_connect
      , $progress_bar
      , $options_hash
    );
    
    if ( $config_manager_type ) {
        
        my $config_manager_class = "Database::ConfigManager::" . $connection_class;
        
        $connection_object->{config_manager} = window::generate(
            $globals
          , $config_manager_class
          , $connection_object
          , $config_manager_type
        );
        
    }

    if ( ! $dont_connect && ! $dont_cache ) {
        $globals->{user_connections}->{ $connection_key } = $connection_object;
    }

    return $connection_object;
    
}

sub connection_type {
    
    my $self = shift;
    
    my $full_class = ref $self;
    
    $full_class =~ /.*::([\w]*)/;
    
    return $1;
    
}

sub connection_label_map {
    
    return {
        Username        => "Username"
      , Password        => "Password"
      , Host_IP         => "Host / IP"
      , Port            => "Port"
      , Database        => ""
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
    };
    
}

sub new {
    
    my ( $class, $globals, $auth_hash, $dont_connect, $progress_bar, $options_hash ) = @_;
    
    my $self;
    
    $self->{globals}   = $globals;
    $self->{auth_hash} = { %$auth_hash }; # We want to store a COPY of the auth_hash, as we might modify it ( Host, Port ) later, but want to remember the original values
    
    bless $self, $class;
    
    $self->{progress_bar} = $progress_bar;
    
    if ( $dont_connect ) {
        return $self;
    } else {
        if ( $self->connect( $auth_hash, $options_hash ) ) {
            $self->{database}  = $auth_hash->{Database}; # It's handy to remember this for later - some DBs have metadata queries that only provide info for the active database
            return $self; 
        } else {
            return undef;
        }
    }
    
}

sub connect {
    
    my ( $self , $auth_hash , $options_hash ) = @_;
    
    # Fill in the port if it's empty
    if ( ! $auth_hash->{Port} ) {
        $auth_hash->{Port} = $self->default_port();
    }
    
    # We call the set up a dynamic proxy if requested ... we need this port early
    if ( $auth_hash->{UseDynamicProxy} ) {
        my $port = $self->setup_dynamic_tunnel( $auth_hash );
        $auth_hash->{Host} = '127.0.0.1';
        $auth_hash->{Port} = $port;
        $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
    }
    
    # Any other pre-connection tasks ...
    ( $auth_hash , $options_hash ) = $self->connect_pre( $auth_hash , $options_hash );
    
    # Actually connect ...
    my $response = $self->connect_do( $auth_hash , $options_hash );
    
    # Finally, any post-connect things ...
    $self->connect_post( $auth_hash , $options_hash );
    
    return $response;
    
}

sub connect_pre {
    
    my ( $self , $auth_hash , $options_hash ) = @_;
    
    return ( $auth_hash , $options_hash );
    
}

sub connect_do {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    if ( ! $auth_hash->{ConnectionString} ) {
        $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
    }
    
    eval {
        $self->{connection} = DBI->connect(
            $auth_hash->{ConnectionString}
          , $auth_hash->{Username}
          , $auth_hash->{Password}
          , $options_hash->{dbi_options_hash}
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

sub connect_post {
    
    my ( $self , $auth_hash , $options_hash ) = @_;
    
    return;
    
}

sub setup_dynamic_tunnel {
    
    my ( $self , $auth_hash , $options_hash ) = @_;
    
    # Find an available port ...
    
    my $available_port;
    
    foreach my $port ( 10000 .. 20000 ) {
        
        my $sock = IO::Socket::INET->new(
            LocalAddr => 'localhost'
          , LocalPort => $port
          , Proto     => 'tcp'
          , ReuseAddr => $^O ne 'MSWin32'
        );
        
        if ( $sock ) {
            close $sock;
            $available_port = $port;
            last;
        }
        
    }
    
    if ( ! $available_port ) {
        $self->dialog(
            {
                title   => "No port free!"
              , type    => "error"
              , text    => "Failed to find an available port. This is suspicious ..."
            }
        );
        return ( undef );
    }
    
    $self->{ssh_tunnel} = Net::OpenSSH->new(
        $auth_hash->{DynamicProxyAddress}
      , master_opts => "-Llocalhost:" . $available_port . ":" . $auth_hash->{Host} . ":" . $auth_hash->{Port}
    ) || die( $! );
    
    return $available_port;
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash, $connection_string ) = @_;
    
    if ( $auth_hash->{UseProxy} ) {
        
        if ( ! $auth_hash->{ProxyAddress} ) {
            
            $self->dialog(
                {
                    title       => "Proxy configuration missing"
                  , type        => "error"
                  , text        => "The UseProxy flag is set, but there is no ProxyAddress!"
                }
            );
            
            return undef;
            
        } else {
            
            my ( $proxy_host, $proxy_port ) = split( /:/, $auth_hash->{ProxyAddress} );
            
            $connection_string = "dbi:Proxy:hostname=$proxy_host;port=$proxy_port;dsn=$connection_string";
            
        }
        
    }
    
    return $connection_string;
    
}

sub default_database {
    
    my $self = shift;
    
    warn $self->connection_type . " doesn't implement default_database()";
    
    return undef;
    
}

sub connection {
    
    my $self = shift;
    
    return $self->{connection};
    
}

sub prepare {
    
    my ( $self, $sql ) = @_;
    
    my $sth;
    
    eval {
        $sth = $self->{connection}->prepare( $sql )
            || confess( $self->{connection}->errstr );
    };
    
    my $err = $@;
    
    if ( $err && ! $self->{globals}->{suppress_error_dialogs} ) {
        $self->dialog(
            {
                title   => "Failed to prepare SQL"
              , type    => "error"
              , text    => $err
            }
        );
        print "\n$sql\n";
        return undef;
    }
    
    $self->{last_prepared_sql} = $sql;
    
    return $sth;
    
}

sub execute {
    
    my ( $self, $sth, $bind_values ) = @_;
    
    my $result;
    
    eval {
        if ( defined $bind_values ) {
            $result = $sth->execute( @$bind_values )
                || confess( $sth->errstr );
        } else {
            $result = $sth->execute
                || confess( $sth->errstr );
        }
    };
    
    my $err = $@;
        
    if ( $err && ! $self->{globals}->{suppress_error_dialogs} ) {
        $self->dialog(
            {
                title   => "Failed to execute SQL"
              , type    => "error"
              , text    => $err
            }
        );
        print "\n" . $self->{last_prepared_sql} . "\n";
    }
    
    return $result;
    
}

sub do {
    
    my ( $self, $sql, $bind_values, $options ) = @_;
    
    my $result;

    if ( $options->{debug} ) {
        print "\n$sql\n\n";
    }

    eval {
        
        if ( defined $bind_values ) {
            $result = $self->{connection}->do( $sql, undef, @$bind_values )
                or confess ( $self->{connection}->errstr );
        } else {
            $result = $self->{connection}->do( $sql )
                or confess ( $self->{connection}->errstr );
        }
        
    };
    
    my $err = $@;
    
    if ( $err && ! $self->{globals}->{suppress_error_dialogs} ) {
        $self->dialog(
            {
                title   => "Failed to execute SQL"
              , type    => "error"
              , text    => $err
            }
        );
        warn $sql;
    }
    
    return $result;
    
}

sub select {
    
    my ( $self, $sql, $bind_values, $key ) = @_;
    
    my $sth = $self->prepare( $sql )
        || return;
    
    if ( $bind_values ) {
        $self->execute( $sth, $bind_values )
            || return;
    } else {
        $self->execute( $sth )
            || return;
    }
    
    my $records = [];
    
    if ( $key ) {
        $records = $sth->fetchall_hashref( $key );
    } else {
        while ( my $row = $sth->fetchrow_hashref ) {
            push @{$records}, $row;
        }
    }
    
    $sth->finish;
    
    return $records;
    
}

sub fetch_database_list {
    
    my $self = shift;
    
    $self->dialog(
        {
            title   => "Oops!"
          , type    => "error"
          , text    => "Can't call Database::Connection::fetch_database_list() ... subclasses must implement this method"
        }
    );
    
    return ();
    
}

sub fetch_schema_list {
    
    my $self = shift;
    
    $self->dialog(
        {
            title   => "Oops!"
          , type    => "error"
          , text    => "Can't call Database::Connection::fetch_schema_list() ... subclasses must implement this method"
        }
    );
    
    return ();
    
}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;
    
    $self->dialog(
        {
            title   => "Oops!"
          , type    => "error"
          , text    => "Can't call Database::Connection::fetch_table_list() ... subclasses must implement this method"
        }
    );
    
    return ();
    
}

sub fetch_materialized_view_list {

    my ( $self, $database, $schema ) = @_;

    return ();

}

sub fetch_field_list {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    my $sth = $self->prepare(
        "select * from " . $self->db_schema_table_string( $database, $schema, $table ) . " where 0=1" );
    
    $self->execute( $sth );
    
    my $fields;
    
    if ( $options->{dont_mangle_case} ) {
        $fields  =$sth->{NAME};
    } else {
        $fields = $sth->{NAME_uc};
    }
    
    $sth->finish();
    
    return $fields;
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    $self->dialog(
        {
            title   => "Oops!"
          , type    => "error"
          , text    => "Can't call Database::Connection::fetch_column_info() ... subclasses must implement this method"
        }
    );
    
    return ();
    
}

sub fetch_view {
    
    my ( $self, $database, $schema, $view ) = @_;
    
    $self->dialog(
        {
            title   => "Oops!"
          , type    => "error"
          , text    => "Can't call Database::Connection::fetch_view() ... subclasses must implement this method"
        }
    );
    
    return ();
    
}

sub count_records_in_table {
    
    my ( $self, $database, $schema, $table, $filter ) = @_;
    
    no warnings 'uninitialized';
    
    my $sth = $self->prepare(
        "select count(*) from " . $self->db_schema_table_string( $database, $schema, $table ) . " $filter"
    ) || return undef;
    
    $self->execute( $sth )
        || return undef;
    
    my $row = $sth->fetchrow_arrayref;
    
    return $$row[0];
    
}

sub unicode_function {
    
    my ( $self, $expression ) = @_;
    
    return $self->UNICODE_FUNCTION . "( $expression )";
    
}

sub get_character_length {
    
    my ( $self, $database, $schema, $table, $column, $filter, $bind_values ) = @_;
    
    my $sql = "select ";
    
    if ( ! $filter ) {
        $sql .= "max(";
    }
    
    $sql .= $self->LENGTH_FUNCTION . "( $column )";
    
    if ( ! $filter ) {
        $sql .= ")";
    }
    
    $sql .="\nfrom "
        . $self->db_schema_table_string( $database, $schema, $table );
    
    if ( $filter ) {
        $sql .= "\n$filter";
    }
    
    my $sth = $self->prepare( $sql )
        || return;
    
    if ( $bind_values ) {
        $self->execute( $sth, $bind_values )
            || return;
    } else {
        $self->execute( $sth )
            || return;
    }
    
    my $row = $sth->fetchrow_arrayref;
    
    if ( $row ) {
        return $$row[0];
    } else {
        return undef;
    }
    
}

sub fetch_dbi_column_info {

    my ( $self, $database, $schema, $table ) = @_;

    # We override this method, because for Netezza ( and possible all DBD::ODBC sources ),
    # after calling $dbh->column_info(), we must *not* call $sth->execute() on the returned $sth
    #  ... whereas for other drivers, the $sth hasn't been executed yet ...

    my $this_dbi_column_info;

    my $sth = $self->prepare(
        "select * from " . $self->db_schema_table_string( $database, $schema, $table ) . " where 0=1" )
      or $self->log->fatal( "Failed to fetch dbi column info!\n" . $self->dbh->errstr );

    $self->execute( $sth );

    my $column_names = $sth->{NAME};

    $sth->finish();

    foreach my $column ( @{$column_names} ) {

        $sth = $self->{connection}->column_info(
            $database
          , $schema
          , $table
          , $column
        ) or die( $self->errstr );

        $self->execute( $sth );

        my $column_info_row = $sth->fetchrow_hashref;

        push @{$this_dbi_column_info}, $column_info_row;

    }

    return $this_dbi_column_info;

}

sub db_schema_string {
    
    my ( $self, $database, $schema, $options ) = @_;
    
    if ( $options->{dont_quote} ) {
        return $database . '.' . $schema;
    } else {
        return '"' . $database . '"."' . $schema . '"';
    }
    
}

sub drop_db_string {
    
    my ( $self , $database , $options ) = @_;
    
    my $sql = "drop database " . $database;
    
    return $sql;
    
}

sub drop_db_schema_string {
    
    my ( $self , $database , $schema , $options ) = @_;
    
    my $sql = "drop schema " . $self->db_schema_string( $database , $schema );
    
    return $sql;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    no warnings 'uninitialized';
    
    if ( defined $database ) {
        return $database . '.' . $schema . '.' . $table;
    } else {
        if ( defined $schema ) {
            return $schema . '.' . $table;
        } else {
            return $table;
        }
    }
    
}

sub drop_db_schema_table_string {

    my ( $self, $database, $schema, $table, $cascade, $options ) = @_;

    my $sql = "drop table " . $self->db_schema_table_string( $database, $schema, $table, $options );

    if ( $cascade ) {
        $sql .= " cascade";
    }

    return $sql;

}

sub truncate_db_schema_table_string {

    my ( $self , $database , $schema , $table , $options ) = @_;

    return "truncate table " . $self->db_schema_table_string( $database, $schema, $table, $options );
    
}

sub create_schema_string {

    my ( $self , $database , $schema ) = @_;

    return "create schema $schema";

}

sub refresh_materialized_view_string {

    my ( $self, $database, $schema, $materialized_view ) = @_;

    return undef;

}

sub md5sum_string {
    
    my ( $self, $expression ) = @_;
    
    $self->dialog(
        {
            title   => "Oops!"
          , type    => "error"
          , text    => "Can't call Database::Connection::md5sum_string() ... subclasses must implement this method"
        }
    );
    
    return undef;
    
}

sub limit_clause {
    
    my ( $self, $row_numbers ) = @_;
    
    return "limit $row_numbers";
    
}

sub limit_select {
    
    my ( $self, $sql, $row_numbers ) = @_;
    
    return $sql . "\nlimit $row_numbers";
    
}

sub sth_2_sqlite {
    
    my ( $self, $sth, $column_defs, $sqlite_dbh, $target_table_name, $progress_bar, $dont_drop ) = @_;
    
    # This function pulls records from $sth ( execute it 1st ) and pushes them into a SQLite DB
    # Muhahahahahaha!
    
    if ( ! $dont_drop ) {
        $sqlite_dbh->do( "drop table if exists $target_table_name" );
    }
        
    my $sql = "create table $target_table_name (\n    ";
    my ( @column_names, @column_def_strings, @placeholders );

    my $sth_names = $sth->{NAME};
    my $sth_types = $sth->{TYPE};
    my $counter = 0;

    if ( ! $column_defs ) {
        foreach my $fieldname ( @${sth_names} ) {
            my $this_type_code = $sth_types->[ $counter ];
            my $this_type = 'text';
            if ( $this_type_code == -6 || $this_type_code == -5 || $this_type_code == 4 or $this_type_code == 5 ) {
                $this_type = 'integer';
            }
            push @{$column_defs}, {
                name        => uc( $fieldname )
              , type        => $this_type
            };
            $counter ++;
        }
    }
    
    foreach my $def ( @{$column_defs} ) {
        push @column_names, $def->{name};
        push @column_def_strings, $def->{name} . " " . $def->{type};
        push @placeholders, "?";
    }
        
    $sql .= join( "\n  , ", @column_def_strings ) . "\n)";
    
    print "\n$sql\n";
    
    if ( ! $dont_drop ) {
        $sqlite_dbh->do( $sql );
    }
    
    $sqlite_dbh->{AutoCommit} = 0;
    
    $sql = "insert into $target_table_name (\n    " . join( "\n  , ", @column_names )
        . "\n) values (\n    " . join( "\n  , ", @placeholders ) . "\n)";
    
    my $insert_sth = $sqlite_dbh->prepare( $sql )
        || die( $sqlite_dbh->errstr );
    
    $counter = 0;
    
    eval {
        while ( my $row = $sth->fetchrow_arrayref ) {
            $counter ++;
            if ( $counter % 500 == 0 ) {
                $sqlite_dbh->{AutoCommit} = 1;
                if ( $progress_bar ) {
                    $progress_bar->set_text( $counter );
                    $progress_bar->pulse;
                    $self->kick_gtk;
                }
                $sqlite_dbh->{AutoCommit} = 0;
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
    
    return $counter;
    
}

sub sql_to_csv {
    
    my ( $self, $options, $progress_bar ) = @_;
    
     # options looks like:
#    {
#        file_path       => $file_path
#      , delimiter       => $delimiter
#      , quote_char      => $quote_char
#      , encoding        => $encoding
#      , column_headers  => 1
#      , sql             => $sql
#      , dont_force_case => 1
#    }
    
    my $csv_writer = Text::CSV->new(
        {
            quote_char              => '"'
          , binary                  => 1
          , eol                     => "\n"
          , sep_char                => $options->{delimiter}
          , escape_char             => '\\'
          , quote_space             => 1
          , blank_is_undef          => 1
          , quote_null              => 1
          , always_quote            => 1
        }
    );
    
    my $writing_directive = ">";
    
    if ( $options->{encoding} ) {
        $writing_directive .= ":encoding(" . $options->{encoding} . ")";
    }
    
    open my $csv_file, $writing_directive, $options->{file_path}
        || $self->dialog(
                {
                    title       => "Failed to open file [" . $options->{file_path} . "] for writing"
                  , type        => "error"
                  , text        => $!
                }
           );
    
    my $sth = $self->prepare( $options->{sql} ) || return;
    
    $self->execute( $sth ) || return;
    
    if ( $options->{column_headers} ) {
        my $fields;
        if ( $options->{dont_force_case} ) {
            $fields = $sth->{NAME};
        } else {
            $fields = $sth->{NAME_uc};
        }
        print $csv_file join( $options->{delimiter}, @{$fields} ) . "\n";
    }
    
    my $counter;
    
    while ( my $page = $sth->fetchall_arrayref( undef, 100 ) ) {
        foreach my $record ( @{$page} ) {
            $csv_writer->print( $csv_file, $record );
            $counter ++;
            if ( $counter % 500 == 0 ) {
                if ( $progress_bar ) {
                    $progress_bar->set_text( $counter );
                    $progress_bar->pulse;
                    $self->kick_gtk;
                }
            }
        }
    };
    
    if ( $progress_bar ) {
        $progress_bar->set_text( "" );
        $progress_bar->set_fraction( 0 );
    }
    
    return $counter;
    
}

# NP Quick convenience wrapper for finding out if database exists..
# TODO Change to allow dynamic checks for various things, not just table..
# Assumes caller's handle. TODO Maybe allow passing an arbitrary handle

sub database_exists {
    
    my ( $self, $database ) = @_;
    
    my $exists;
    
    # Calls the subclass' method to grab tables
    foreach my $this_database ( $self->fetch_database_list() ) {
        
        if ( $this_database eq $database ) {
            $exists = 1;
            last;
        }
        
    }
    
    return $exists;
    
}

sub table_exists {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    my $exists;
    
    foreach my $this_table ( $self->fetch_table_list( $database, $schema ) ) {
        
        if ( $this_table eq $table ) {
            $exists = 1;
            last;
        }
        
    }
    
    return $exists;
    
}

sub has_schemas {
    
    my $self = shift;
    
    return TRUE;
    
}

sub is_sql_database {
    
    my $self = shift;
    
    return TRUE;
    
}

sub can_execute_ddl {
    
    my $self = shift;
    
    return TRUE;
    
}

sub can_alias {
    
    my $self = shift;
    
    return TRUE;
    
}

sub can_ddl_in_transaction {

    my $self = shift;

    return FALSE;

}

sub can_create_database {

    my $self = shift;

    return TRUE;

}

sub has_odbc_driver {

    my $self = shift;

    return FALSE;

}

sub default_object_case {

    my $self = shift;

    # This is the default case that objects are created in, if unquoted.
    # Classes should return one of [ 'lower', 'upper', undef ]
    # ( undef meaning even if unquoted, case will not be mangled )

    return 'lower';

}

# all the below code is for managing the model metadata - for the migration wizard and db browser

sub create_model_schema {
    
    my $self = shift;
    
    $self->dialog(
        {
            title       => "Can't create model schema"
          , type        => "error"
          , text        => "Subclasses must implement this method!"
        }
    );
    
    return undef;
    
}

sub generate_current_activity_query {
    
    my $self = shift;
    
    return "-- NOT YET IMPLEMENTED FOR THIS DATABASE TYPE";
    
}

sub mangle_case_result_data {
    
    my ( $self, $results, $case ) = @_;
    
    # This method mangles the case of *data* in a resultset
    # We would do this when ( for example ) dealing with column name metadata from
    # 2 databases that return differing case
    
    # Note that this code isn't particularly well optimised, but is fine
    # for small(ish) recordsets
    
    my $return;
    
    if ( ref $results eq 'HASH' ) {
        
        foreach my $key ( keys %{$results} ) {
            $return->{ $key } = $self->_mangle_case_record_data( $results->{ $key }, $case )
        }
        
    } elsif ( ref $results eq 'ARRAYREF' ) {
        
        foreach my $record( @{$results} ) {
            push @{$return}, $self->_mangle_case_record_data( $record, $case );
        }
        
    }
    
    return $return;
    
}

sub _mangle_case_record_data {
    
    my ( $self, $input_record, $case ) = @_;
    
    my $output_record;
    
    if ( ref $input_record eq 'HASH' ) {
        
        foreach my $this_record_key ( keys %{$input_record} ) {
            
            if ( $case eq 'upper' ) {
                $output_record->{ $this_record_key } = uc( $input_record->{ $this_record_key } );
            } else {
                $output_record->{ $this_record_key } = lc( $input_record->{ $this_record_key } );
            }
            
        }
        
    } elsif ( ref $input_record eq 'ARRAY' ) {
        
        foreach my $value ( @{$input_record} ) {
            
            if ( $case eq 'upper' ) {
                push @{$output_record}, uc( $value );
            } else {
                push @{$output_record}, lc( $value );
            }
            
        }
        
    }
    
    return $output_record;
    
}

sub _mangle_model_case {
    
    my ( $self, $case ) = @_;
    
    $self->do(
        "update target_db_schemas set\n"
      . "    database_name         = " . $case . "( database_name )\n"
      . "  , schema_name           = " . $case . "( schema_name )\n"
    ) || return;
    
    $self->do(
        "update tables set\n"
      . "    database_name         = " . $case . "( database_name )\n"
      . "  , schema_name           = " . $case . "( schema_name )\n"
      . "  , table_name            = " . $case . "( table_name )\n"
      . "  , val_database_name     = " . $case . "( val_database_name )\n"
      . "  , val_schema_name       = " . $case . "( val_schema_name )\n"
      . "  , val_table_name        = " . $case . "( val_table_name )\n"
    ) || return;
    
    $self->do(
        "update views set\n"
      . "    database_name         = " . $case . "( database_name )\n"
      . "  , schema_name           = " . $case . "( schema_name )\n"
      . "  , view_name             = " . $case . "( view_name )\n"
      . "  , view_definition       = " . $case . "( view_definition )\n" # TODO - might need more than this
    ) || return;
    
    $self->do(
        "update table_columns set\n"
      . "    database_name         = " . $case . "( database_name )\n"
      . "  , schema_name           = " . $case . "( schema_name )\n"
      . "  , table_name            = " . $case . "( table_name )\n"
      . "  , column_name           = " . $case . "( column_name )\n"
    ) || return;
    
    $self->do(
        "update indexes set\n"
      . "    database_name         = " . $case . "( database_name )\n"
      . "  , schema_name           = " . $case . "( schema_name )\n"
      . "  , table_name            = " . $case . "( table_name )\n"
      . "  , index_name            = " . $case . "( index_name )\n"
    ) || return;
    
    $self->do(
        "update index_columns set\n"
      . "    column_name           = " . $case . "( column_name )\n"
    ) || return;
    
    $self->do(
        "update fk_rels set\n"
      . "    relationship_name     = " . $case . "( relationship_name )\n"
      . "  , primary_database      = " . $case . "( primary_database )\n"
      . "  , primary_schema        = " . $case . "( primary_schema )\n"
      . "  , primary_table         = " . $case . "( primary_table )\n"
      . "  , foreign_database      = " . $case . "( foreign_database )\n"
      . "  , foreign_schema        = " . $case . "( foreign_schema )\n"
      . "  , foreign_table         = " . $case . "( foreign_table )\n"
    ) || return;
    
    $self->do(
        "update fk_rel_parts set\n"
      . "    primary_column        = " . $case . "( primary_column )\n"
      . "  , foreign_column        = " . $case . "( foreign_column )\n"
    ) || return;

    $self->do(
        "update sequences set\n"
      . "    database_name         = " . $case . "( database_name )\n"
      . "  , schema_name           = " . $case . "( schema_name )\n"
      . "  , table_name            = " . $case . "( table_name )\n"
      . "  , sequence_name         = " . $case . "( sequence_name )\n"
    ) || return;

}

sub hash_to_table {

    my ( $self, $hash, $table ) = @_;

    my ( @keys , @values );

    foreach my $key ( keys %{$hash} ) {
        push @keys, $key;
        push @values, $hash->{$key};
    }

    my $sth = $self->prepare(
        "insert into $table\n"
      . "(\n"
      . join( "\n  , ", @keys )
      . "\n) values (\n    "
      . join( "\n  , ", ('?')x@keys )
      . "\n)"
    ) || return;

    $self->execute(
        $sth
      , \@values
    ) || return;

    return 1;

}

# NP    Grab all of the indexes from the source connection/database, and insert them 
#       into the approp sqlite memdb table, ready for creation of target DDLs, use in GUI etc

sub _model_fetch_and_populate_indexes {
    
    my ( $self, $source_dbh, $source_database_name, $source_schema_name ) = @_;
    
    my $index_info = $source_dbh->fetch_all_indexes( $source_database_name, $source_schema_name );
    
    while ( my ( $index_name, $index_structure ) = each %{$index_info} ) {

        my ( $mapped_db , $mapped_schema , $mapped_table ) = $self->{globals}->{windows}->{'window::migration_wizard'}->map_schema( $source_database_name , $source_schema_name , $index_structure->{TABLE_NAME} );
        # NP Execute pre-prepared handle setup in SQLite 'create_model_schema' while fetching metadata
        $self->{_model_index_insert_sth}->execute(
            $mapped_db
          , $mapped_schema
          , $mapped_table
          , $index_name
          , $index_structure->{IS_PRIMARY}
          , $index_structure->{IS_UNIQUE}
          , ( $index_structure->{IS_DISTRIBUTION_KEY} ? $index_structure->{IS_DISTRIBUTION_KEY} : 0 )
          , ( $index_structure->{IS_ORGANISATION_KEY} ? $index_structure->{IS_ORGANISATION_KEY} : 0 )
        );

        my $index_id = $self->last_insert_id(
            undef
          , undef
          , "indexes"
          , undef
        );
        
        foreach my $column_name ( @{$index_structure->{COLUMNS}} ) {
            $self->{_model_index_columns_insert_sth}->execute(
                $index_id
              , $column_name
            );
        }
        
    }
    
}

sub _model_fetch_and_populate_sequences {

    my ( $self, $source_dbh, $source_database_name, $source_schema_name , $mapper ) = @_;

    my $sequence_info = $source_dbh->fetch_all_sequences( $source_database_name, $source_schema_name );

    my $issues;

    while ( my ( $sequence_name, $sequence_structure ) = each %{$sequence_info} ) {

        my ( $mapped_db , $mapped_schema , $mapped_table ) = $self->{globals}->{windows}->{'window::migration_wizard'}->map_schema( $source_database_name , $source_schema_name , $sequence_structure->{table_name} );

        my ( $target_sequence_type , $note );

        ( $target_sequence_type , $issues , $note ) = $mapper->map_column( $source_dbh , $source_database_name , $source_schema_name , $sequence_structure->{table_name}
                                                                       , $sequence_structure->{sequence_type} , undef , undef , 0 , $issues , $note );

        # NP Execute pre-prepared handle setup in SQLite 'create_model_schema' while fetching metadata
        $self->{_model_sequence_insert_sth}->execute(
            $mapped_db
          , $mapped_schema
          , $mapped_table
          , $sequence_name
          , $sequence_structure->{sequence_type}
          , $target_sequence_type
          , $sequence_structure->{sequence_increment}
          , $sequence_structure->{sequence_min}
          , $sequence_structure->{sequence_max}
          , $sequence_structure->{extra_options}
          , $note
        );

    }

}

sub _model_fetch_and_populate_fks {
    
    my ( $self, $source_dbh, $source_database_name, $source_schema_name ) = @_;

    # NP Call the subclass' method to grab its foreign keys

    my $foreign_key_info = $source_dbh->fetch_all_foreign_key_info( $source_database_name, $source_schema_name );
    
    while ( my ( $relationship_name, $relationship_structure ) = each %{$foreign_key_info} ) {

        my ( $mapped_primary_db , $mapped_primary_schema , $mapped_primary_table ) = $self->{globals}->{windows}->{'window::migration_wizard'}->map_schema( $source_database_name , $relationship_structure->{PRIMARY_SCHEMA} , $relationship_structure->{PRIMARY_TABLE} );
        my ( $mapped_ref_db , $mapped_ref_schema , $mapped_ref_table ) = $self->{globals}->{windows}->{'window::migration_wizard'}->map_schema( $source_database_name , $relationship_structure->{REFERENCED_SCHEMA} , $relationship_structure->{REFERENCED_TABLE} );

        # NP Execute pre-prepared handle setup in SQLite's 'create_model_schema' while fetching metadata
        $self->{_model_fk_rel_insert_sth}->execute(
            $relationship_name
          , $mapped_primary_db
          , $mapped_primary_schema
          , $mapped_primary_table
          , $mapped_ref_db                       # TODO: is it possible to have FKs across 2 databases ???
          , $mapped_ref_schema
          , $mapped_ref_table
        );
        
        my $fk_id = $self->last_insert_id(
            undef
          , undef
          , "fk_rels"
          , undef
        );
        
        foreach my $column_pair ( @{$relationship_structure->{RELATIONSHIP_COLUMNS}} ) {
            $self->{_model_fk_rel_part_insert_sth}->execute(
                $fk_id
              , $column_pair->{PRIMARY_COLUMN}
              , $column_pair->{REFERENCED_COLUMN}
            );
            
        }
        
    }
    
}

sub _model_fetch_and_populate_view_definitions {
    
    my ( $self, $source_dbh, $source_database_name, $source_schema_name ) = @_;
    
    my $all_view_definitions = $source_dbh->fetch_all_view_definitions( $source_database_name, $source_schema_name );
    
    while ( my ( $view_name, $view_definition_hash ) = each %{$all_view_definitions} ) {

        my ( $mapped_db , $mapped_schema , $mapped_view ) = $self->{globals}->{windows}->{'window::migration_wizard'}->map_schema( $source_database_name , $source_schema_name , $view_name );

        $self->{_model_view_insert_sth}->execute(
            $source_database_name
          , $source_schema_name
          , $view_name
          , $mapped_db
          , $mapped_schema
          , $mapped_view
          , $view_definition_hash->{VIEW_DEFINITION}
        );

    }
    
}

sub _model_to_fk_structure {
    
    my ( $self, $mem_dbh, $database, $schema, $fk_constraint_name ) = @_;
    
    # Note: we currently ignore the $database and $schema as the migration wizard ( which populates
    #       the database in $mem_dbh ) ONLY populates for a single given database/schema. But in the
    #       future, we might want to pull in a list of databases and schemas into the model, and so
    #       we'll *then* have to filter on db/schema
    
    # This method translates metadata from the model in $mem_dbh into a structure, which can then be
    # used by subclasses to do things like output a DDL to represent the constraint
    
    my $fk_rel_tables_rows = $mem_dbh->select(
        "select\n"
      . "            ID\n"
      . "          , primary_database\n"
      . "          , primary_schema\n"
      . "          , primary_table\n"
      . "          , foreign_database\n"
      . "          , foreign_schema\n"
      . "          , foreign_table\n"
      . "from\n"
      . "            fk_rels\n"
      . "where\n"
      . "        primary_database  = ?\n"
      . "    and ( primary_schema  = ? or primary_schema is null )\n" # kinda dodgy, but it will work
      . "    and relationship_name = ?"
      , [  $database, $schema, $fk_constraint_name ]
    );
    
    my $return = {
        PRIMARY_DATABASE    => $fk_rel_tables_rows->[0]->{primary_database}
      , PRIMARY_SCHEMA      => $fk_rel_tables_rows->[0]->{primary_schema}
      , PRIMARY_TABLE       => $fk_rel_tables_rows->[0]->{primary_table}
      , FOREIGN_DATABASE    => $fk_rel_tables_rows->[0]->{foreign_database}
      , FOREIGN_SCHEMA      => $fk_rel_tables_rows->[0]->{foreign_schema}
      , FOREIGN_TABLE       => $fk_rel_tables_rows->[0]->{foreign_table}
    };
    
    my $fk_rel_columns = $mem_dbh->select(
        "select\n"
      . "            primary_column\n"
      . "          , foreign_column\n"
      . "from\n"
      . "            fk_rel_parts\n"
      . "where\n"
      . "            fk_rel_id = ?"
      , [ $fk_rel_tables_rows->[0]->{ID} ]
    );
    
    my ( @primary_columns, @foreign_columns );
    
    foreach my $column_pair ( @{$fk_rel_columns} ) {
        
        push @primary_columns, $column_pair->{primary_column};
        push @foreign_columns, $column_pair->{foreign_column};
        
    }
    
    $return->{PRIMARY_COLUMNS} = \@primary_columns;
    $return->{FOREIGN_COLUMNS} = \@foreign_columns;
    
    return $return;
    
}

sub _model_to_primary_key_structure {

    my ( $self, $database, $schema, $table ) = @_;

    my $pk_rows = $self->select(
        "select\n"
      . "            column_name\n"
      . "from\n"
      . "            indexes\n"
      . "inner join  index_columns\n"
      . "                                 on indexes.ID = index_columns.index_ID\n"
      . "where\n"
      . "            indexes.database_name = ?\n"
      . "        and ( indexes.schema_name = ? or indexes.schema_name is null )\n"
      . "        and indexes.table_name    = ?\n"
      . "        and indexes.is_primary    = 1"
      , [ $database , $schema , $table ]
    );

    return $pk_rows;
    
}

sub _model_to_unique_key_structure {

    my ( $self, $database, $schema, $table ) = @_;

    my $pk_rows = $self->select(
        "select\n"
      . "            column_name\n"
      . "from\n"
      . "            indexes\n"
      . "inner join  index_columns\n"
      . "                                 on indexes.ID = index_columns.index_ID\n"
      . "where\n"
      . "            indexes.database_name = ?\n"
      . "        and ( indexes.schema_name = ? or indexes.schema_name is null )\n"
      . "        and indexes.table_name    = ?\n"
      . "        and indexes.is_unique     = 1"
      , [ $database , $schema , $table ]
    );

    return $pk_rows;

}

sub _model_to_index_structure {
    
    my ( $self, $database, $schema, $table, $index_name ) = @_;
    
    my $sql =       "select\n"
      . "            column_name\n"
      . "from\n"
      . "            indexes\n"
      . "inner join  index_columns\n"
      . "                                 on indexes.ID = index_columns.index_ID\n"
      . "where\n"
      . "            indexes.database_name       = ?\n"
      . "        and ( indexes.schema_name       = ? or indexes.schema_name is null )\n" # kinda dodgy, but it will work
      . "        and indexes.table_name          = ?\n"
      . "        and indexes.index_name          = ?\n"
      . "--        and indexes.is_primary          = 0\n" # commented out, so DB2 can rewire _model_to_primary_key_ddl to _model_to_index_ddl
      . "--        and indexes.is_distribution_key = 0\n" # commented out as it's preventing dist key indexes which are *also* unique from being generated ( which prevents FKs being created )
      . "--        and indexes.is_organisation_key = 0";
    
    my $index_rows = $self->select(
        $sql
      , [ $database , $schema , $table , $index_name ]
    );
    
    return $index_rows;
    
}

sub _model_to_organisation_key_structure {

    my ( $self, $database, $schema, $table ) = @_;

    my $sql =       "select\n"
      . "            column_name\n"
      . "from\n"
      . "            indexes\n"
      . "inner join  index_columns\n"
      . "                                 on indexes.ID = index_columns.index_ID\n"
      . "where\n"
      . "            indexes.database_name       = ?\n"
      . "        and ( indexes.schema_name       = ? or indexes.schema_name is null )\n" # kinda dodgy, but it will work
      . "        and indexes.table_name          = ?\n"
      . "        and indexes.is_organisation_key = 1\n";

    my $index_rows = $self->select(
        $sql
      , [ $database , $schema , $table ]
    );

    my $return = [];

    foreach my $row ( @{$index_rows} ) {
        push @{$return} , $row->{column_name};
    }

    return $return;

}

sub _model_to_distribution_key_structure {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    my $sql =       "select\n"
      . "            column_name\n"
      . "from\n"
      . "            indexes\n"
      . "inner join  index_columns\n"
      . "                                 on indexes.ID = index_columns.index_ID\n"
      . "where\n"
      . "            indexes.database_name       = ?\n"
      . "        and ( indexes.schema_name       = ? or indexes.schema_name is null )\n" # kinda dodgy, but it will work
      . "        and indexes.table_name          = ?\n"
      . "        and indexes.is_distribution_key = 1";
    
    my $distribution_key_rows = $self->select(
        $sql
      , [ $database , $schema , $table ]
    );

    if ( ! @{$distribution_key_rows} ) {

        # If we fail to locate distribution keys, we first fall back to using the primary key ...
        $distribution_key_rows = $self->_model_to_primary_key_structure( $database, $schema, $table );

        # If that fails too, we fall back to using the 1st unique key we find ...
        if ( ! @{$distribution_key_rows} ) {

            $sql = "select\n"
                 . "            index_name\n"
                 . "from\n"
                 . "            indexes\n"
                 . "where\n"
                 . "            indexes.database_name       = ?\n"
                 . "        and ( indexes.schema_name       = ? or indexes.schema_name is null )\n" # kinda dodgy, but it will work
                 . "        and indexes.table_name          = ?\n"
                 . "        and indexes.is_unique = 1";

            my $all_unique_indexes_for_table = $self->select(
                $sql
              , [ $database , $schema , $table ]
            );

            if ( @{$all_unique_indexes_for_table} ) {

                $sql = "select\n"
                     . "            column_name\n"
                     . "from\n"
                     . "            indexes\n"
                     . "inner join  index_columns\n"
                     . "                                 on indexes.ID = index_columns.index_ID\n"
                     . "where\n"
                     . "            indexes.database_name       = ?\n"
                     . "        and ( indexes.schema_name       = ? or indexes.schema_name is null )\n" # kinda dodgy, but it will work
                     . "        and indexes.table_name          = ?\n"
                     . "        and indexes.index_name          = ?";

                $distribution_key_rows = $self->select(
                    $sql
                  , [ $database , $schema , $table , $all_unique_indexes_for_table->[0]->{index_name} ]
                );

            }

        }

    }

    return $distribution_key_rows;
    
}

sub _model_to_primary_or_distribution_key_structure {

    my ( $self, $database, $schema, $table ) = @_;

    # In some cases, we just want some KEYS ... whether they be primary or unique or distribution keys, it matters not.
    my $keys = $self->_model_to_primary_key_structure( $database , $schema , $table );
    
    if ( ! @{$keys} ) {
        $keys = $self->_model_to_unique_key_structure( $database , $schema , $table );
    }
    
    if ( ! @{$keys} ) {
        $keys = $self->_model_to_distribution_key_structure( $database , $schema , $table );
    }

    return $keys;

}

sub _model_to_table_ddl {
    
    # This *might* be all we need for most databases ...
    # Subclasses can either override completely, or they can make use of this logic via $self->SUPER::_model_to_table_dll
    # and then apply their own changes ( eg adding distribution keys )
    
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
    
    my $sql = "create table "
        . $self->db_schema_table_string( $object_recordset->{database_name}, $object_recordset->{schema_name}, $object_recordset->{table_name} ) . "\n"
        . "(\n";
    
    my $counter;

    my $col_max_length  = 40;
    my $type_max_length = 30;
    
    foreach my $column_mapper ( @{$columns_and_mappers} ) {
        
        # print spaces / commas @ start of line
        $sql .= $counter ? '  , ' : '    ';
        
        # the column name
        $sql .= $column_mapper->{column_name} . ' ' . ( ' ' x ( $col_max_length - length( $column_mapper->{column_name} ) + 1 ) );
        
        # the type
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
        
        $sql .= ( $final_type || '' );
        
        my $precision_scale = exists $mangled_return->{precision_scale}
                                   ? $mangled_return->{precision_scale}
                                   : $column_mapper->{column_precision};
        
        if ( $precision_scale ) {
            $sql .= $precision_scale;
        }
        
        if ( ! $column_mapper->{column_nullable} ) {
            no warnings 'uninitialized';
            $sql .= ( ' ' x ( $type_max_length - length( $final_type . $precision_scale ) + 1 ) ) . " not null";
        }
        
        $sql .= "\n";
        
        $counter ++;
        
    }
    
    $sql .= ")";
    
    return {
        ddl         => $sql
      , warnings    => join( "\n\n", @warnings )
    };
    
}

sub _model_to_primary_key_ddl {
    
    my $self = shift;
    
    return {
        ddl         => ""
      , warnings    => "_model_to_primary_key_ddl() not implemented for this target database"
    };
    
}

sub _model_to_index_ddl {
    
    my $self = shift;
    
    return {
        ddl         => ""
      , warnings    => "_model_to_index_ddl() not implemented for this target database"
    };
    
}

sub _model_to_sequence_ddl {

    my $self = shift;

    return {
        ddl         => ""
      , warnings    => "_model_to_sequence_ddl() not implemented for this target database"
    }

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
            . "add constraint " . $object_recordset->{relationship_name} . "\n"
            . "foreign key    ( " . join( " , ", @{$fk_structure->{PRIMARY_COLUMNS}} ) . " )\n"
            . "references     $foreign_db_schema_table ( " . join( " , ", @{$fk_structure->{FOREIGN_COLUMNS}} ) . " )\n"; # NP TODO There's a "Use of uninitialized value in concatenation"

    return {
        ddl         => $sql
      , warnings    => undef
    };

}

#sub _model_to_fk_rel_ddl {
#    
#    my $self = shift;
#    
#    return {
#        ddl         => ""
#      , warnings    => "_model_to_fk_rel_ddl() not implemented for this target database"
#    };
#    
#}

sub _model_to_view_ddl {
    
    my ( $self, $control_dbh, $object_recordset ) = @_;

    my $view_definition = "create or replace view " . $self->db_schema_table_string(
        $object_recordset->{database_name}
      , $object_recordset->{schema_name}
      , $object_recordset->{view_name}
    ) . " as \n" . $object_recordset->{view_definition};

    # TODO: push into metadata:
    # # Convert square brackets ( ie [] ) to quotes, *and* force text inside the square brackets to upper-case
    #
    # $view_definition =~ s/\[([\w]*)\]/'"' . uc($1) . '"'/ge;
    #
    # # NP Now Look for nolocks directive in DDL
    #
    # if ( $view_definition =~ /nolock/im ) {
    #
    #     #warn "View $view_name is using 'with (nolock)'... So just checking for other options to try and preserve them if need be";
    #
    #     # NP Alrighty, let's have a closer look... if it's just nolock in the with clause get rid of it completely, including the 'with (.*)'..
    #     # If not, try preserving the other options (I'm not sure what other options or delimiter is available here, this is used at table level a lot - which would be harder to parse out)
    #
    #     my $exp = 'with\s*?\((.*?)\s*?nolock(.*?\))';
    #
    #     if ( $view_definition =~ /$exp/im ) {
    #
    #         # NP This is fairly fragile... :/
    #         if ( $1 && $2 =~ /\)/ ) {
    #
    #             # we need to preserve the other stuff
    #             warn "Hang on a sec.. I don't know what to do about option(s) $1 yet..";
    #
    #         } else {
    #
    #             #$view_definition =~ s/with\s*?\(\s*?nolock.*?\)//img;
    #             $view_definition =~ s/$exp//img;
    #
    #         }
    #
    #     }
    #
    # }

    return {
        ddl         => $view_definition
      , warnings    => undef
    };
    
}

sub _max_dist_key_elements {

    my $self = shift;

    return 4;

}

sub _distribute_verb {

    my $self = shift;

    return "distribute on";

}

sub object_alias_string {

    my ( $self , $object , $alias ) = @_;

    return "$object as $alias";

}

sub _pks_to_distribution_string {

    my ( $self , $primary_keys ) = @_;

    my @pk_items;
    my $sql;
    my @warnings;

    my $max_dist_key_elements = $self->_max_dist_key_elements();

    if ( @{$primary_keys} ) {

        $sql = " " . $self->_distribute_verb() . " ( ";

        my $column_count = 0;

        foreach my $row ( @{$primary_keys} ) {

            $column_count ++;

            if ( $column_count > $max_dist_key_elements ) {
                push @warnings, "Skipping > $max_dist_key_elements ( currently defined max ) key components";
                last;
            }

            push @pk_items, $row->{column_name};

        }

        $sql .= join( " , ", @pk_items ) . " )\n";

    } else {

        $sql = "";

    }

    return {
        ddl       => $sql
      , warnings  => join( "\n\n", @warnings )
    };

}

sub reserved_words {

    my $self = shift;

    return qw | select from table column index user update where
        ABORT DEC LEADING RESET DECIMAL LEFT REUSE AGGREGATE DECODE LIKE RIGHT ALIGN DEFAULT LIMIT ROWS ALL DEFERRABLE LISTEN ROWSETLIMIT ALLOCATE DESC LOAD RULE ANALYSE DISTINCT LOCAL SEARCH ANALYZE DISTRIBUTE LOCK SELECT AND DO MATERIALIZED SEQUENCE ANY ELSE MINUS SESSION_USER AS END MOVE SETOF ASC EXCEPT NATURAL SHOW
        BETWEEN EXCLUDE NCHAR SOME BINARY EXISTS NEW SUBSTRING BIT EXPLAIN NOT SYSTEM BOTH EXPRESS NOTNULL TABLE CASE EXTEND NULL THEN
        CAST EXTERNAL NULLIF TIES CHAR EXTRACT NULLS TIME CHARACTER FALSE NUMERIC TIMESTAMP CHECK FIRST NVL TO CLUSTER FLOAT NVL2 TRAILING
        COALESCE FOLLOWING OFF TRANSACTION COLLATE FOR OFFSET TRIGGER COLLATION FOREIGN OLD TRIM COLUMN FROM ON TRUE CONSTRAINT FULL ONLINE UNBOUNDED
        COPY FUNCTION ONLY UNION  CROSS GENSTATS OR UNIQUE CURRENT GLOBAL ORDER USER CURRENT_CATALOG GROUP OTHERS USING CURRENT_DATE HAVING OUT VACUUM
        CURRENT_DB IDENTIFIER_CASE OUTER VARCHAR CURRENT_SCHEMA ILIKE OVER VERBOSE CURRENT_SID IN OVERLAPS VERSION CURRENT_TIME INDEX PARTITION VIEW CURRENT_TIMESTAMP
        INITIALLY POSITION WHEN CURRENT_USER INNER PRECEDING WHERE CURRENT_USERID INOUT PRECISION WITH CURRENT_USEROID INTERSECT PRESERVE WRITE DEALLOCATE INTERVAL PRIMARY RESET
        INTO REUSE CTID  OID XMIN CMIN XMAX CMAX TABLEOID ROWID DATASLICEID CREATEXID DELETEXID DATE
    |;

}

sub connection_browse_title {

    my $self = shift;

    return undef;

}

1;
