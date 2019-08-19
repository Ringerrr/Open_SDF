package SmartAssociates::Globals;

use strict;
use warnings;

use Cwd;
use File::Find::Rule;

use base 'SmartAssociates::Base';

my $IDX_LOG                                     =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  0;

my $IDX_ROOTDIR                                 =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  1;
my $IDX_LOGDIR                                  =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  2;
my $IDX_CONTROL_DB_NAME                         =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  3;
my $IDX_LOG_DB_NAME                             =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  4;
my $IDX_BATCH                                   =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  5;
my $IDX_JOB                                     =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  6;
my $IDX_TMP_FILES                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  7;
my $IDX_PARAMETER_RECURSION_LIMIT               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  8;
my $IDX_ACTIVE_PROCESSES                        =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX +  9;
my $IDX_MAX_CONCURRENT_PROCESSES                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 10;
my $IDX_PID_TO_JOB_ID_MAPPING                   =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 11;
my $IDX_MAX_ERRORS                              =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 12;
my $IDX_EXTRACT_DATE                            =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 13;
my $IDX_CHAIN_LOAD_FILENAME                     =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 14;
my $IDX_EXTRACT_DIR                             =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 15;
my $IDX_CONFIG_GROUP                            =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 16;
my $IDX_ITERATORS                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 17;
my $IDX_ARRAYS                                  =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 18;
my $IDX_QUERY_PARAMETERS                        =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 19;
my $IDX_MISC                                    =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 20;
my $IDX_LOCAL_CONFIG_DB                         =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 21;
my $IDX_SIMPLE_SELECT_STH                       =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 22;
my $IDX_CONN_NAME_TO_DB_TYPE_MAP                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 23;
my $IDX_FIFO_BASE                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 24;
my $IDX_DIR_SEPARATOR                           =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 25;
my $IDX_SMART_CONFIG_PATH                       =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 26;
my $IDX_LAST_STEP_RECORDS_AFFECTED              =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 27;
my $IDX_DISABLE_FIFO                            =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 28;
my $IDX_USER_PROFILE                            =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 29;
my $IDX_CURRENT_TEMPLATE_CONFIG                 =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 30;
my $IDX_COMMAND_LINE_ARGS                       =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 31;
my $IDX_RESOLVERS                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 32;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 33;

# This class is the base class of all other objects in the SmartAssociates ETL framework.
# It holds global variables, accessor methods, and other handy stuff for
# other classes to inherit.

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self = [];
    
    bless $self, $class;
    
    # Initialise the MISC attribute. This is a hash that we can chuck 'stuff' into
    # without having to add to the formal structure. DO NOT abuse ...
    
    $self->MISC( {} );
    $self->TMP_FILES( [] );
    $self->ITERATORS( {} );
    $self->ACTIVE_PROCESSES( 0 );
    $self->[ $IDX_RESOLVERS ] = [];

    return $self;
    
}

sub initialise {
    
    my $self = shift;
    
    # We do this here because we want to be able to call ->log ... but the log
    # object needs a connection to US.
    
    # Create a connection to the local config database. This currently has a
    # 'connections' table, which stores credentials to all the databases we
    # connect to
    
    my $config_db_path = $self->[ $IDX_SMART_CONFIG_PATH ];
    
    if ( ! -e $config_db_path ) {
        $self->LOG->fatal( "The config database [$config_db_path] doesn't exist!" );
    }
    
    $self->[ $IDX_LOCAL_CONFIG_DB ] = DBI->connect( "dbi:SQLite:dbname=" . $config_db_path, "", "" )
        || $self->LOG->fatal( "Could not connect to Smart ETL's configuration database\n" . DBI->errstr );
    
    # Prepare the 'simple select' statement handle, which is used by SIMPLE_SELECT() to fetch
    # records from the key/value store
    $self->[ $IDX_SIMPLE_SELECT_STH ] = $self->[ $IDX_LOCAL_CONFIG_DB ]->prepare(
        "select ID, value from simple_config where key = ?"
    ) || $self->LOG->fatal( "Failed to prepare SimpleSelect from Smart ETL's configuration database\n" . DBI->errstr );
    
    # Create a hash of connection names to DB types. We use this to determine what class of connection
    # object to construct, based on a connection name
    my $sth = $self->[ $IDX_LOCAL_CONFIG_DB ]->prepare(
        "select ConnectionName, DatabaseType from connections"
    ) || $self->LOG->fatal( "Failed to fetch connection name to db type mapping!\n" . $self->[ $IDX_LOCAL_CONFIG_DB ]->errstr );
    
    $sth->execute()
        || $self->LOG->fatal( "Failed to fetch connection name to db type mapping!\n" . $sth->errstr );
    
    $self->[ $IDX_CONN_NAME_TO_DB_TYPE_MAP ] = $sth->fetchall_hashref( "ConnectionName" );

    # Fetch environment variables from simple_config and set them
    $sth = $self->[ $IDX_LOCAL_CONFIG_DB ]->prepare(
        "select * from simple_config where key like 'ENV%'"
    ) || $self->LOG->fatal( "Failed to fetch environment variables!\n" . $self->[ $IDX_LOCAL_CONFIG_DB ]->errstr );

    $sth->execute()
        || $self->LOG->fatal( "Failed to fetch environment variables!\n" . $sth->errstr );

    while ( my $env_row = $sth->fetchrow_hashref ) {

        my ( $env_key , $value ) = ( $env_row->{key} , $env_row->{value} );
        my $env;

        if ( $env_key =~ /^ENV:(.*)/ ) {
            $env = $1;
            $self->LOG->info( "Setting environment variable [$env] to [$value]" );
            $ENV{$env} = $value;
        } else {
            $self->LOG->warn( "Failed to parse environment string [$env_key]" );
        }

    }

    # Create an array of resolvers ( #COMPLEX# param logic ). # We scan all ETL paths ( ie base and overlays )
    # for everything under SmartAssociates/Resolver/* and instantiate each class we find. This allows users to easily
    # add more resolver logic without having to register classes. There shouldn't be any real hierarchy to the logic,
    # but having separate classes allows us to keep all logic related to a particular use-case all together in one
    # location and not cluttering up a huge all-in-one file.

    foreach my $path ( $self->ALL_ETL_PATHS() ) {
        my @these_resolver_classes;
        push @these_resolver_classes , File::Find::Rule->file()
                                                       ->name( "*" )
                                                       ->in( $path . "/SmartAssociates/Resolver" );
        foreach my $this_class_name ( @these_resolver_classes ) {
            if ( $this_class_name =~ /.*\/([\w]*)\.pm$/ ) {
                $this_class_name = $1;
                $self->LOG->info( "Adding resolver class [$this_class_name] from [$path/SmartAssociates/Resolver]" );
                my $this_class = SmartAssociates::Base::generate(
                    $self
                  , 'SmartAssociates::Resolver::' . $this_class_name
                  , $this_class_name
                );
                push @{ $self->[ $IDX_RESOLVERS ] } , $this_class;
            }
        }
    }

}

sub ITERATOR {
    
    my ( $self, $iterator_name, $iterator ) = @_;
    
    # Here we implement a simple iterator manager
    
    if ( ! $iterator ) {
        
        if ( exists $self->[ $IDX_ITERATORS ]->{ $iterator_name } ) {
            
            return $self->[ $IDX_ITERATORS ]->{ $iterator_name };
            
        } else {
            
            $self->LOG->fatal( "Attempt to access undefined iterator: [$iterator_name]" );
            
        }
        
    } else {
        
        $self->[ $IDX_ITERATORS ]->{ $iterator_name } = $iterator;
        return $self->[ $IDX_ITERATORS ]->{ $iterator_name };
        
    }
    
}

sub ARRAY {

    my ( $self, $array_name, $array ) = @_;

    # Here we implement a simple array manager

    if ( ! $array ) {

        if ( exists $self->[ $IDX_ARRAYS ]->{ $array_name } ) {

            return $self->[ $IDX_ARRAYS ]->{ $array_name };

        } else {

            $self->LOG->fatal( "Attempt to access undefined array: [$array_name]" );

        }

    } else {

        $self->[ $IDX_ARRAYS ]->{ $array_name } = $array;
        return $self->[ $IDX_ARRAYS ]->{ $array_name };

    }

}

sub SIMPLE_SELECT {
    
    my ( $self, $key ) = @_;
    
    $self->[ $IDX_SIMPLE_SELECT_STH ]->execute( $key )
        || $self->log->fatal( "SIMPLE_SELECT failed:\n" . $self->[ $IDX_SIMPLE_SELECT_STH ]->errstr );
    
    my $exists = $self->[ $IDX_SIMPLE_SELECT_STH ]->fetchrow_hashref;
    
    if ( $exists ) {
        
        return $exists->{value};
        
    } else {
        
        return undef;
        
    }
    
    
}

sub CONNECTION_NAME_TO_DB_TYPE {
    
    my ( $self, $connection_name ) = @_;
    
    if ( exists $self->[ $IDX_CONN_NAME_TO_DB_TYPE_MAP ]->{ $connection_name } ) {
        
        return $self->[ $IDX_CONN_NAME_TO_DB_TYPE_MAP ]->{ $connection_name }->{DatabaseType};
        
    } elsif ( $connection_name eq 'Memory' ) {
        
        return 'Memory'; # hard-coded for now ... should it be in our config.db?
        
    } else {
        
        $self->LOG->fatal( "I was asked to return a database type from connection name [$connection_name],"
                         . " but this connection does not exist in the configuration database!" );
        
    }
    
}

sub ETL_PATH {
    
    my $self = shift;
    
    return cwd;
    
}

sub ETL_OVERLAY_PATHS {
    
    my $self = shift;
    
    my $sth = $self->[ $IDX_LOCAL_CONFIG_DB ]->prepare(
        "select OverlayPath from etl_overlays where Active = 1"
    ) || $self->LOG->fatal( $self->[ $IDX_LOCAL_CONFIG_DB ]->errstr );
    
    $sth->execute()
        || $self->LOG->fatal( $sth->errstr );
    
    my @return;
    
    while ( my $path = $sth->fetchrow_hashref ) {
        push @return, $path->{OverlayPath};
    }
    
    $sth->finish;
    
    return @return;
    
}

sub ALL_ETL_PATHS {
    
    my $self = shift;
    
    my @all_paths;
    
    push @all_paths, $self->ETL_PATH;
    
    my @overlay_paths = $self->ETL_OVERLAY_PATHS;
    
    if ( @overlay_paths ) {
        push @all_paths, @overlay_paths;
    }
    
    return @all_paths;
    
}

sub LOG                         { return $_[0]->accessor( $IDX_LOG,                         $_[1] ); }
sub LOGDIR                      { return $_[0]->accessor( $IDX_LOGDIR,                      $_[1] ); }
sub ROOTDIR                     { return $_[0]->accessor( $IDX_ROOTDIR,                     $_[1] ); }
sub CONTROL_DB_NAME             { return $_[0]->accessor( $IDX_CONTROL_DB_NAME,             $_[1] ); }
sub LOG_DB_NAME                 { return $_[0]->accessor( $IDX_LOG_DB_NAME,                 $_[1] ); }
sub BATCH                       { return $_[0]->accessor( $IDX_BATCH,                       $_[1] ); }
sub JOB                         { return $_[0]->accessor( $IDX_JOB,                         $_[1] ); }
sub TMP_FILES                   { return $_[0]->accessor( $IDX_TMP_FILES,                   $_[1] ); }
sub PARAMETER_RECURSION_LIMIT   { return $_[0]->accessor( $IDX_PARAMETER_RECURSION_LIMIT,   $_[1] ); }
sub ACTIVE_PROCESSES            { return $_[0]->accessor( $IDX_ACTIVE_PROCESSES,            $_[1] ); }
sub MAX_CONCURRENT_PROCESSES    { return $_[0]->accessor( $IDX_MAX_CONCURRENT_PROCESSES,    $_[1] ); }
sub PID_TO_JOB_ID_MAPPING       { return $_[0]->accessor( $IDX_PID_TO_JOB_ID_MAPPING,       $_[1] ); }
sub MAX_ERRORS                  { return $_[0]->accessor( $IDX_MAX_ERRORS,                  $_[1] ); }
sub CHAIN_LOAD_FILENAME         { return $_[0]->accessor( $IDX_CHAIN_LOAD_FILENAME,         $_[1] ); }
sub EXTRACT_DATE                { return $_[0]->accessor( $IDX_EXTRACT_DATE,                $_[1] ); }
sub EXTRACT_DIR                 { return $_[0]->accessor( $IDX_EXTRACT_DIR,                 $_[1] ); }
sub CONFIG_GROUP                { return $_[0]->accessor( $IDX_CONFIG_GROUP,                $_[1] ); }
sub ITERATORS                   { return $_[0]->accessor( $IDX_ITERATORS,                   $_[1] ); }
sub Q_PARAMS                    { return $_[0]->accessor( $IDX_QUERY_PARAMETERS,            $_[1] ); }
sub LOCAL_CONFIG_DB             { return $_[0]->accessor( $IDX_LOCAL_CONFIG_DB,             $_[1] ); }
sub FIFO_BASE                   { return $_[0]->accessor( $IDX_FIFO_BASE,                   $_[1] ); }
sub DIR_SEPARATOR               { return $_[0]->accessor( $IDX_DIR_SEPARATOR,               $_[1] ); }
sub SMART_CONFIG_PATH           { return $_[0]->accessor( $IDX_SMART_CONFIG_PATH,           $_[1] ); }
sub LAST_STEP_RECORD_AFFECTED   { return $_[0]->accessor( $IDX_LAST_STEP_RECORDS_AFFECTED,  $_[1] ); }
sub DISABLE_FIFO                { return $_[0]->accessor( $IDX_DISABLE_FIFO,                $_[1] ); }
sub USER_PROFILE                { return $_[0]->accessor( $IDX_USER_PROFILE,                $_[1] ); }
sub CURRENT_TEMPLATE_CONFIG     { return $_[0]->accessor( $IDX_CURRENT_TEMPLATE_CONFIG,     $_[1] ); }
sub COMMAND_LINE_ARGS           { return $_[0]->accessor( $IDX_COMMAND_LINE_ARGS,           $_[1] ); }
sub RESOLVERS                   { return $_[0]->accessor( $IDX_RESOLVERS,                   $_[1] ); }

sub MISC                        { return $_[0]->accessor( $IDX_MISC,                        $_[1] ); }

1;
