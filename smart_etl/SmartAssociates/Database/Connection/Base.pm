package SmartAssociates::Database::Connection::Base;

use strict;
use warnings;

use Carp;

use IO::Socket::INET;
use Net::OpenSSH;
use JSON;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' DB_TYPE ';                    # subclasses define this 

use base 'SmartAssociates::Base';

my $IDX_DBH                                         =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  0;
my $IDX_FIELDS_CACHE                                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  1;
my $IDX_FIELD_METADATA_CACHE                        =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  2;
my $IDX_IN_TRANSACTION                              =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  3;
my $IDX_COLUMN_TYPE_CODE_CACHE                      =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  4;
my $IDX_COLUMN_PRECISION_CACHE                      =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  5;
my $IDX_CREDENTIALS                                 =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  6;
my $IDX_CURRENT_DATABASE                            =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  7;
my $IDX_SSH_TUNNEL                                  =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  8;
my $IDX_FIELD_FORMATTING_CACHE                      =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  9;

use constant    FIRST_SUBCLASS_INDEX                => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 10;

use constant    PERL_ZERO_RECORDS_INSERTED          => '0E0';

# These constants refer to data items inside a return to a $dbh->column_info() call:
#  http://search.cpan.org/~timb/DBI-1.623/DBI.pm#column_info
# We use an array and not hash here for speed ( in DataCop we look up lots of items per record )

use constant    COLUMN_INFO_TABLE_CAT               =>   0;      # database
use constant    COLUMN_INFO_TABLE_SCHEM             =>   1;      # schema
use constant    COLUMN_INFO_TABLE_NAME              =>   2;      # table
use constant    COLUMN_INFO_COLUMN_NAME             =>   3;      # column name
use constant    COLUMN_INFO_DATA_TYPE               =>   4;      # smallint type
use constant    COLUMN_INFO_TYPE_NAME               =>   5;      # text type
use constant    COLUMN_INFO_COLUMN_SIZE             =>   6;      # maximum length for %CHAR,  number of digits or bits for numeric data types
use constant    COLUMN_INFO_BUFFER_LENGTH           =>   7;      # length in bytes of transferred data
use constant    COLUMN_INFO_DECIMAL_DIGITS          =>   8;      # total number of significant digits to the right of the decimal point
use constant    COLUMN_INFO_NUM_PREC_RADIX          =>   9;      # radix for numeric precision - value is 10 or 2 for numeric data types
use constant    COLUMN_INFO_NULLABLE                =>  10;      # 0: not null, 1: nullable, 2: unknown
use constant    COLUMN_INFO_REMARKS                 =>  11;      # remarks
use constant    COLUMN_INFO_COLUMN_DEF              =>  12;      # default
use constant    COLUMN_INFO_SQL_DATA_TYPE           =>  13;      # the sql data type
use constant    COLUMN_INFO_SQL_DATETIME_SUB        =>  14;      # subtype code for datetime and interval data types
use constant    COLUMN_INFO_CHAR_OCTET_LENGTH       =>  15;      # maximum length in bytes of a character or binary data type column
use constant    COLUMN_INFO_ORDINAL_POSITION        =>  16;      # column sequence number (starting with 1)
use constant    COLUMN_INFO_IS_NULLABLE             =>  17;      # indicates if the column can accept NULLs. Possible values are: 'NO', 'YES' and ''
# NOTE: other columns will ( have already ) appeared at the end of this from the DBI subsystem. Do NOT pack things at the end ...

# Unfortunately, the rest of the info columns are database-dependant :(
# We have to use negative indexing to get to our INT subtype ...

use constant    COLUMN_INFO_SCALE                   =>  -3;
use constant    COLUMN_INFO_PRECICION               =>  -2;
use constant    COLUMN_INFO_INT_SUBTYPE             =>  -1;

# These constants are for our 'subtype', and allow us to do a quick integer comparison to
# determine if a detected column type requirement exceeds the current column definition.
# ie this is an optimisation
use constant    BYTEINT                             => 0;
use constant    TINYINT                             => 1;
use constant    SMALLINT                            => 2;
use constant    INT                                 => 3;
use constant    BIGINT                              => 4;

# These constants are the max allowed value for the INT types. Note that these are appropriate for POSTGRES.
# They are currently used in the DataCop class.
# Subclasses should override these if necessary.

use constant    BYTEINT_MAX                         => 0;
use constant    SMALLINT_MAX                        => 327688;
use constant    INTEGER_MAX                         => 2147483648;
use constant    BIGINT_MAX                          => "9223372036854775808";

# These constants are Perl's integer typecodes for SQL column types ...
# 'done' ones are handled in DataCop
use constant    SQL_GUID                            => -11;
use constant    SQL_WLONGVARCHAR                    => -10;
use constant    SQL_WVARCHAR                        =>  -9;     # done
use constant    SQL_WCHAR                           =>  -8;     # done
use constant    SQL_BIGINT                          =>  -5;     # done
use constant    SQL_BIT                             =>  -7;
use constant    SQL_TINYINT                         =>  -6;     # done
use constant    SQL_LONGVARBINARY                   =>  -4;
use constant    SQL_VARBINARY                       =>  -3;
use constant    SQL_BINARY                          =>  -2;
use constant    SQL_LONGVARCHAR                     =>  -1;
use constant    SQL_UNKNOWN_TYPE                    =>   0;
use constant    SQL_ALL_TYPES                       =>   0;
use constant    SQL_CHAR                            =>   1;     # done
use constant    SQL_NUMERIC                         =>   2;     # done
use constant    SQL_DECIMAL                         =>   3;     # done
use constant    SQL_INTEGER                         =>   4;     # done
use constant    SQL_SMALLINT                        =>   5;     # done
use constant    SQL_FLOAT                           =>   6;
use constant    SQL_REAL                            =>   7;
use constant    SQL_DOUBLE                          =>   8;
use constant    SQL_DATETIME                        =>   9;     # done
use constant    SQL_DATE                            =>   9;     # done
use constant    SQL_INTERVAL                        =>  10;
use constant    SQL_TIME                            =>  10;
use constant    SQL_TIMESTAMP                       =>  11;     # done
use constant    SQL_VARCHAR                         =>  12;     # done
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

sub generate {
    
    # This STATIC FUNCTION ( not a method ) will determine which subclass of
    # SmartAssociates::Database::Connection we need, and construct an object of that type
    
    my $globals                 = $_[0];
    my $connection_name         = $_[1];
    my $database                = $_[2];
    
    my $connection_class        = $globals->CONNECTION_NAME_TO_DB_TYPE( $connection_name );
    
    my $object_class            = 'SmartAssociates::Database::Connection::' . $connection_class;

    my $connection_object       = SmartAssociates::Base::generate(
                                      $globals
                                    , $object_class
                                    , $connection_name
                                    , $database
                                  );

    $connection_object->current_database( $database );

    return $connection_object;
    
}

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1] );
    
    my $connection_name         = $_[2]     || $self->log->fatal( "Missing arg: connection_name" );
    my $database                = $_[3]     || $self->log->fatal( "Missing arg: database" );
    
    $self->[ $IDX_FIELDS_CACHE ]            = {};
    $self->[ $IDX_COLUMN_TYPE_CODE_CACHE ]  = {};
    $self->[ $IDX_FIELD_FORMATTING_CACHE ]  = {};

    my $auth_hash = {};
    
    # We don't have credentials for the in-memory database ...
    if ( $self->DB_TYPE ne &SmartAssociates::Database::Connection::Memory::DB_TYPE ) {
        
        $auth_hash = $self->get_credentials(
            $connection_name
          , $self->DB_TYPE
        );
        
    }
    
#    my $connection_string = $self->build_connection_string( $auth_hash, $database );
    
    $auth_hash->{Database} = $database;
    
    my $options_hash = {};
    
    $self->connect( $auth_hash , $options_hash );
    
    return $self;
    
}

sub default_port {
    
    my $self = shift;
    
    return -1;
    
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
    $self->connect_do( $auth_hash , $options_hash );
    
    # Finally, any post-connect things ...
    $self->connect_post( $auth_hash , $options_hash );
    
}

sub connect_pre {
    
    my ( $self , $auth_hash , $options_hash ) = @_;
    
    return ( $auth_hash , $options_hash );
    
}

sub connect_do {
    
    my ( $self, $auth_hash , $options_hash ) = @_;

    # Netezza is defaulting to the SYSTEM database if we don't regenerate ...
#    if ( ! $auth_hash->{ConnectionString} ) {
        $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
#    }
    
    my $dbh = DBI->connect(
        $auth_hash->{ConnectionString}
      , $auth_hash->{Username}
      , $auth_hash->{Password}
      , $options_hash->{dbi_options_hash}
    ) || $self->log->fatal( "Could not connect to " . $self->DB_TYPE . " database\n" . DBI->errstr );
    
    $self->dbh( $dbh );
    
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
        $self->log->warn( "setup_dynamic_tunnel() failed to find an available port. This is suspicious ..." );
        return undef;
    }
    
    $self->[ $IDX_SSH_TUNNEL ] = Net::OpenSSH->new(
        $auth_hash->{DynamicProxyAddress}
      , master_opts => "-Llocalhost:" . $available_port . ":" . $auth_hash->{Host} . ":" . $auth_hash->{Port}
    ) || $self->log->fatal( $! );
    
    return $available_port;
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash, $connection_string ) = @_;
    
    if ( $auth_hash->{UseProxy} ) {
        
        if ( ! $auth_hash->{ProxyAddress} ) {
            
            $self->log->fatal( "The UseProxy flag is set, but there is no ProxyAddress!" );
            
        } else {
            
            my ( $proxy_host, $proxy_port ) = split( /:/, $auth_hash->{ProxyAddress} );
            
            $connection_string = "dbi:Proxy:hostname=$proxy_host;port=$proxy_port;dsn=$connection_string";
            
        }
        
    }
    
    return $connection_string;
    
}

sub prepare {
    
    # This function is a wrapper around DBI's prepare() method
    # We log the SQL being prepared, and call log->fatal() on an error
    
    my ( $self, $sql ) = @_;
    
    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )
    
    # We do this basically so we can log the line of the CALLER that requested a database prepare()
    #  ... rather than just logging our own position inside this function
    
    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );
    
    # We only want to print a line break ( ie ====== ) of 60 characters or less
    my $sql_length = length( $sql );
    my $line_length = $sql_length < 60 ? $sql_length : 60;
    
    $self->log->debug(
        "Preparing SQL:\n" . ( '=' x $line_length )
        . "\n" . $sql
        . "\n" . ( '=' x $line_length )
        , $line
        , $filename
    );
    
    my $sth;
    
    # Do NOT eval this. Callers should be doing this
    #eval {
        $sth = $self->[ $IDX_DBH ]->prepare( $sql )
            || croak( $self->[ $IDX_DBH ]->errstr );
    #};
    
    #if ( $@ ) {
    #    $self->log->fatal( "Error preparing statement:\n$sql\n" . ( '=' x $line_length ) . "\n" . $@, $line );
    #}
    
    return $sth;
    
}

sub execute {
    
    # This function is a wrapper around DBI's execute() method
    # We log the SQL being executed and call log->fatal() on an error
    
    my ( $self, $sth, $bind_array ) = @_;
    
    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )
    
    # We do this basically so we can log the line of the CALLER that requested a database execute()
    #  ... rather than just logging our own position inside this function
    
    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );
    
    # We only want to print a line break ( ie ====== ) of 60 characters or less
    my $sql_length = length( $sth->{Statement} );
    my $line_length = $sql_length < 60 ? $sql_length : 60;
    
    my $debug_log_line = "Executing SQL:\n" . ( '=' x $line_length )
                       . "\n" . $sth->{Statement}
                       . "\n" . ( '=' x $line_length );
    
    if ( $bind_array ) {
        no warnings 'uninitialized';
        $debug_log_line .= "\nwith the values: \n[\n   " . join( "\n , ", @{$bind_array} ) . "\n]\n" . ( '=' x $line_length );
    }
    
    $self->log->debug(
        $debug_log_line
      , $line
      , $filename
    );
    
    # The response to an execute statement is generally the number of records affected, or 0E0 for 0 records affected
    my $response;
    
    # NOTE: we ONLY catch serialization errors here. For everything else, the caller should catch errors and do the load_execution logging
    
    my $retries = 0;
    my $success = 0;
    
    while ( $retries < 5 && ! $success ) {
        
        if ( $retries ) {
            
            my $sleep_seconds = $retries * 5;
            $self->log->info( "Retrying after a serialization error ... first sleeping for [$sleep_seconds] seconds ..." );
            sleep( $sleep_seconds );
        }
        
        eval {
            if ( $bind_array ) {
                $response = $sth->execute( @{$bind_array} )
                    || croak( $sth->errstr, $line );
            } else {
                $response = $sth->execute()
                    || croak( $sth->errstr, $line );
            }
        };
        
        my $error = $@;
        
        if ( $error ) {
            
            if ( $error =~ /ERROR:  Concurrent update or delete of same row/ig ) {
                
                $self->log->warn( "\n\n****************************************************************\n"
                                    . "Caught a serialization error:\n$error."
                                    . "****************************************************************" );
                
                $retries ++;
                
            } else {
                
                # Any error that's not a serialization issue is fatal ( and should be caught by a caller )
                
#                croak( $sth->errstr, $line );
                croak( $error, $line );
                
            }
            
        }  else {
            
            # No error. Set $success, which exist the retry loop
            
            $success = 1;
            
        }
        
    }
    
    return $response;
    
}

sub do {
    
    # This function is a wrapper around DBI's do() method
    # We log the SQL being executed and call $self->log->fatal() on an error
    
    my ( $self, $sql ) = @_;
    
    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )
    
    # We do this basically so we can log the line of the CALLER that requested a database execute()
    #  ... rather than just logging our own position inside this function
    
    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );
    
    # We only want to print a line break ( ie ====== ) of 60 characters or less
    my $sql_length = length( $sql );
    my $line_length = $sql_length < 60 ? $sql_length : 60;
    
    my $debug_log_line = "Executing SQL:\n" . ( '=' x $line_length ) . "\n" . $sql . "\n" . ( '=' x $line_length );
    
    $self->log->debug(
        $debug_log_line
      , $line
      , $filename
    );
    
    # The response to an execute statement is generally the number of records affected, or 0E0 for 0 records affected
    my $response;
    
    # Do NOT eval this. The caller should do this, and do LOAD_EXECUTION logging
#    eval {
        $response = $self->[ $IDX_DBH ]->do( $sql )
            || die( $self->[ $IDX_DBH ]->errstr, $line );
#    };
    
#    if ( $@ ) {
#        $self->log->fatal( $@ );
#    }
    
    return $response;
    
}

sub commit {
    
    my $self = shift;
    
    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )
    
    # We do this basically so we can log the line of the CALLER that requested a database execute()
    #  ... rather than just logging our own position inside this function
    
    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );
    
    eval {
        $self->[ $IDX_DBH ]->commit()
            || die( $self->[ $IDX_DBH ]->errstr, $line );
    };
    
    if ( $@ ) {
        $self->log->fatal( $@ );
    }
    
    $self->[ $IDX_IN_TRANSACTION ] = 0;
    
}

sub begin_work {
    
    my $self = shift;
    
    # This line pulls in information about the caller, so we can
    # include things like the line number in the log message
    # Most items aren't used, but are left in place for clarity ( caller() returns 11 items )
    
    # We do this basically so we can log the line of the CALLER that requested a database execute()
    #  ... rather than just logging our own position inside this function
    
    #       0         1         2          3         4
    my ( $package, $filename, $line, $subroutine, $hasargs,
    #    5            6         7          8         9         10
    $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    = caller( 1 );
    
    if ( $self->[ $IDX_IN_TRANSACTION ] ) {
        $self->log->debug( "This template is set as transactionable, but we're already in a transaction" );
        return;
    }
    
    $self->[ $IDX_IN_TRANSACTION ] = 1;
    
    eval {
        $self->[ $IDX_DBH ]->begin_work()
            || die( $self->[ $IDX_DBH ]->errstr, $line );
    };
    
    my $err = $@;
    
    if ( $err ) {
        
        if ( $err =~ /Already in a transaction/ ) {
            $self->log->warn( $err );
        } else {
            $self->log->fatal( $err );
        }
    }
    
}

sub disconnect {
    
    my $self = shift;
    
    $self->dbh->disconnect();
    
}

sub get_fields_from_table {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    my $db_schema_table = $self->db_schema_table_string( $database, $schema, $table );
    
    if ( ! exists $self->[ $IDX_FIELDS_CACHE ]->{ $db_schema_table } ) {
        
        my $sth = $self->prepare(
            "select * from $db_schema_table where 0=1"
        );
        
        $self->execute( $sth );
        
        $self->[ $IDX_FIELDS_CACHE ]->{ $db_schema_table } = $sth->{NAME};
        
        $sth->finish();
        
    }
    
    return $self->[ $IDX_FIELDS_CACHE ]->{ $db_schema_table };
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table , $options ) = @_;
    
    if ( ! $options->{dont_quote} ) {
    
        if ( defined $database ) {
            return $database . '.' . $schema . '.' . $table;
        } elsif ( defined $schema ) {
            return $schema . '.' . $table;
        } elsif ( defined $table ) {
            return $table;
        } else {
            $self->log->fatal( "db_schema_table_string() wasn't passed anything" );
        }

    } else {
        
        if ( defined $database ) {
            return '"' . $database . '"."' . $schema . '"."' . $table . '"';
        } elsif ( defined $schema ) {
            return '"' . $schema . '"."' . $table . '"';
        } elsif ( defined $table ) {
            return '"' . $table . '"';
        } else {
            $self->log->fatal( "db_schema_table_string() wasn't passed anything" );
        }
    }
    
}

sub db_schema_string {

    my ( $self , $database , $schema , $options ) = @_;

    if ( ! $options->{dont_quote} ) {

        if ( defined $database ) {
            return $database . '.' . $schema;
        } elsif ( defined $schema ) {
            return $schema;
        } else {
            $self->log->fatal( "db_schema_string() wasn't passed anything" );
        }

    } else {

        if ( defined $database ) {
            return '"' . $database . '"."' . $schema . '"';
        } elsif ( defined $schema ) {
            return '"' . $schema . '"';
        } else {
            $self->log->fatal( "db_schema_string() wasn't passed anything" );
        }
    }

}

sub distribution_key_string {

    my ( $self , $keys ) = @_;

    return "";

}

sub create_schema_string {

    my ( $self , $database , $schema ) = @_;

    my $sql = "create schema " . $self->db_schema_string( $database , $schema );

    return $sql;

}

sub truncate_db_schema_table_string {

    my ( $self, $database, $schema, $table ) = @_;

    return "truncate " . $self->db_schema_table_string( $database , $schema , $table );

}

sub fetch_dbi_column_info {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    my $this_dbi_column_info;
    
    my $sth = $self->prepare(
        "select * from " . $self->db_schema_table_string( $database, $schema, $table ) . " where 0=1" )
      or $self->log->fatal( "Failed to fetch dbi column info!\n" . $self->dbh->errstr );
    
    $sth->execute
        or $self->log->fatal( "Failed to fetch dbi column info!\n" . $self->dbh->errstr );
    
    my $column_names = $sth->{NAME};
    
    my $perl_type_to_integer_sub_type_mapping = {
        &SQL_BIGINT      => BIGINT
      , &SQL_TINYINT     => TINYINT
      , &SQL_INTEGER     => INT
      , &SQL_SMALLINT    => SMALLINT
    };
    
    foreach my $column ( @{$column_names} ) {
        
        $sth = $self->dbh->column_info(
            $database
          , $schema
          , $table
          , $column
        ) or $self->log->fatal( $self->dbh->errstr );
        
        $sth->execute
            or $self->log->fatal( $sth->errstr );
        
        my @column_info_row;
        
        push @column_info_row, @{$sth->fetchrow_arrayref};
        
        # This is STUPID. The COLUMN_SIZE definition is BACKWARDS.
        my $precision_scale = $column_info_row[ COLUMN_INFO_COLUMN_SIZE ];
        
        my ( $scale, $precision );
        
        if ( $precision_scale =~ /([\d]*),([\d]*)/ ) {
            ( $scale, $precision ) = ( $2, $1 );
        }
        
        push @column_info_row, $scale, $precision;
        
        push @column_info_row, $perl_type_to_integer_sub_type_mapping->{ $column_info_row[ COLUMN_INFO_DATA_TYPE ] };
        
        push @{$this_dbi_column_info}, \@column_info_row;
        
    }
    
    return $this_dbi_column_info;
        
}

sub fetch_column_type_info {
    
    my ( $self, $database, $schema, $table, $column , $usage ) = @_;
    
    # This method is currently used by parts of the migration wizard, to drive the generation
    # of SQL expressions per column. New code should use fetch_column_definitions, which also fetches
    # lengths, scale, etc, and we should port code that uses this method and then remove this method

    if ( ! $usage ) {
        $self->log->fatal( "fetch_column_type_info() called without a usage" );
    }

    my $unquoted_column = $column;
    $unquoted_column =~ s/"//g;
    
    if ( ! exists $self->[ $IDX_COLUMN_TYPE_CODE_CACHE ]->{$database}->{$schema}->{$table} ) {
        
        my $sth = $self->prepare(
            "select * from " . $self->db_schema_table_string( $database, $schema, $table ) . " where 0=1" );
        
        $self->execute( $sth );
        
        my $column_names      = $sth->{NAME};
        my $column_types      = $sth->{TYPE};
        my $column_precisions = $sth->{PRECISION};
        my $column_scales     = $sth->{SCALE};
        
        my $type_codes;
        my $precisions;
        
        my $counter = 0;
        
        foreach my $column_name ( @{$column_names} ) {
            $type_codes->{$column_name} = $$column_types[$counter];
            $precisions->{$column_name}->{precision} = $$column_precisions[$counter];
            $precisions->{$column_name}->{scale} = $$column_scales[$counter];
            $counter ++;
        }
        
        $self->[ $IDX_COLUMN_TYPE_CODE_CACHE ]->{$database}->{$schema}->{$table} = $type_codes;
        $self->[ $IDX_COLUMN_PRECISION_CACHE ]->{$database}->{$schema}->{$table} = $precisions;
        
    }
    
    if ( ! exists $self->[ $IDX_COLUMN_TYPE_CODE_CACHE ]->{$database}->{$schema}->{$table} ) {
        
        $self->log->warn( "Couldn't locate metadata for column" );
        
        return {
            type_code           => undef
          , formatted_select    => $column
          , precision           => undef
        };
        
    } else {
        
        my $type_code = $self->[ $IDX_COLUMN_TYPE_CODE_CACHE ]->{$database}->{$schema}->{$table}->{$unquoted_column};
        my $precision = $self->[ $IDX_COLUMN_PRECISION_CACHE ]->{$database}->{$schema}->{$table}->{$unquoted_column};
        
        return {
            type_code           => $type_code
          , formatted_select    => $self->formatted_select( $database , $schema , $table , $column , $type_code , $usage )
          , precision           => $precision
        };
        
    }
    
}

sub formatted_select {

    my ( $self, $database, $schema, $table, $column_name, $type_code , $usage ) = @_;

    $self->log->debug( "Looking up formatting string for db: [$database] schema: [$schema] table: [$table] column: [$column_name] type code: [$type_code] usage: [$usage]" );

    if ( ! exists $self->[ $IDX_FIELD_FORMATTING_CACHE ]->{ $database }->{ $schema }->{ $table }->{$usage} ) {
        my $control_dbh = $self->globals->CONFIG_GROUP->dbh();
        my $sth = $control_dbh->prepare( "select * from db_formatting_strings where db_type = ? and usage = ?" );
        my $db_type = $self->DB_TYPE();
        $db_type =~ s/::.*//g; # Strip out subclasses - we store formatting strings against the 'main' DB class
        $control_dbh->execute( $sth , [ $db_type , $usage ] );
        $self->[ $IDX_FIELD_FORMATTING_CACHE ]->{ $database }->{ $schema }->{ $table }->{ $usage } = $sth->fetchall_hashref( "TYPE_CODE" );
        $self->log->info( "formatted_select() fetched metadata for table [$table] and usage type[$usage]:" . to_json( $self->[ $IDX_FIELD_FORMATTING_CACHE ]->{ $database }->{ $schema }->{ $table }->{ $usage } , { pretty => 1 } ) );
    }

    my $select_string = $column_name; # The default, if we find no formatting string, is to just return the column name back

    if ( exists $self->[ $IDX_FIELD_FORMATTING_CACHE ]->{ $database }->{ $schema }->{ $table }->{ $usage }->{ $type_code } ) {
        my $formatting_string_record = $self->[ $IDX_FIELD_FORMATTING_CACHE ]->{ $database }->{ $schema }->{ $table }->{ $usage }->{ $type_code };
        $select_string = $formatting_string_record->{SELECT_STRING};
        $select_string =~ s/(\$\w+)/$1/eeg; # evaluate the $column_name variable in the string we fetch from the DB
        $self->log->debug( "Found formatting string metadata ... returning [$select_string] from:\n" . to_json( $formatting_string_record , { pretty => 1 } ) );
    } else {
        $self->log->debug( "No formatting string metadata found ... returning [$select_string]" );
    }

    return $select_string;

}

sub coalesce {
    
    my ( $self , $expression , $string ) = @_;
    
    return "coalesce($expression,'$string')";
    
}

sub concatenate {

    my $self    = shift;
    my $options = shift;

    # NP TODO Change use/require of Data::Dumper to only when debug flag set etc
    #warn "In Target concat routine.. Have been passed in expressions: " . Dumper ($options);

    # Note: This concatentation of all cols excludes the primary keys, they'll be added on our return, and used for uniqueness V speed
    # NP Note also on netezza this means that creation of the primary keys is imperitive before doing the comparison, even though they're not used..
    #    Or, there needs to be a way of telling which fields to not include

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

        # Now, match any numbery type thingies (from type list DK has in oracle.pm) and alter if needed

        if ( $exp->{type_code} ~~ [-7..-5, 2..8] ) {
            # no need to alias individual cols/expressions; they'll be concated as part of larger expression
            $this_expression = "cast ($this_expression as VARCHAR(20))";
        }

        # TODO: optimise - don't coalesce NOT NULL columns
        push @all_expressions, $self->coalesce( $this_expression ,"''" );

    }

    my $return = join ' || ', @all_expressions;

    return $return;

}

sub encrypt_expression {
    
    my ( $self, $expression ) = @_;
    
    $self->log->fatal( "encrypt_expression() is not implemented for this target database!" );
    
}

sub get_credentials {
    
    my ( $self, $connection_name, $database_type ) = @_;
    
    $self->log->info(
        "Fetching credentials for:\n"
      . " ConnectionName: [$connection_name]\n"
      . " DatabaseType:   [$database_type]\n"
    );
    
    my $dbh = $self->globals->LOCAL_CONFIG_DB;
    
    my $sth;
    
    eval {
        
        $sth = $dbh->prepare(
            "select * from connections\n"
          . "where  ConnectionName = ? and DatabaseType = ?"
        ) || die( $dbh->errstr );
        
        $sth->execute( $connection_name , $database_type )
            || die( $sth->errstr );
        
    };
    
    my $err = $@;
    
    if ( $@ ) {
        $self->log->fatal( "Failed to fetch credentials from configuration database:\n$err" );
    }
    
    my $auth_hash = $sth->fetchrow_hashref;
    
    if ( ! $auth_hash ) {
        $self->log->fatal(
            "Requested connection doesn't exist in the configuration database."
          . "Please add this connection to the configuration database ..."
        );
    }
    
    $self->credentials( $auth_hash );
    
    return $auth_hash;
    
}

sub simple_select {
    
    my ( $self , $key ) = @_;
    
    my $sth = $self->prepare(
        "select value from simple_config where key = ?"
    ) || $self->LOG->fatal( "Failed to prepare simple_select from Smart ETL's configuration database\n" . DBI->errstr );
    
    $self->execute( $sth , [ $key ] );
    
    my $exists = $sth->fetchrow_hashref;
    
    if ( $exists ) {
        return $exists->{value};
    } else {
        return undef;
    }
    
}

sub dbh                     { return $_[0]->accessor( $IDX_DBH,                     $_[1] ); }
sub fields_cache            { return $_[0]->accessor( $IDX_FIELDS_CACHE,            $_[1] ); }
sub field_metadata_cache    { return $_[0]->accessor( $IDX_FIELD_METADATA_CACHE,    $_[1] ); }
sub column_type_code_cache  { return $_[0]->accessor( $IDX_COLUMN_TYPE_CODE_CACHE,  $_[1] ); }
sub credentials             { return $_[0]->accessor( $IDX_CREDENTIALS,             $_[1] ); }
sub current_database        { return $_[0]->accessor( $IDX_CURRENT_DATABASE,        $_[1] ); }

1;
