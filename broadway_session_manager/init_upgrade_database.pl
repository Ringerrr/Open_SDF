#!/usr/bin/perl -w

use strict;
use warnings;

use Cwd;
use DBI;
use Digest::MD5 qw(md5_hex);
use Term::ReadKey;
use Getopt::Long;

use constant    PERL_ZERO_RECORDS_INSERTED      => '0E0';

#use Data::Dumper;

my $reset_admin_user;

GetOptions(
    'reset-admin-user'   => \$reset_admin_user
);

my $config_dir;

if ( $ENV{'XDG_CONFIG_HOME'} ) {
    $config_dir = $ENV{'XDG_CONFIG_HOME'};
} else {
    $config_dir = $ENV{"HOME"} . "/.broadway_session_manager";
}

print "Config dir: [$config_dir]\n";

if ( ! -d $config_dir ) {
    mkdir( $config_dir )
        || die( "Failed to create config directory [$config_dir]:\n" . $! );
}

# Turn off output buffering.
# This is so stuff gets dumped to the console / app log immediately instead of somewhat after the event.
select STDERR;
$| = 1;

select STDOUT;
$| = 1;

# Connect to the config database
print "Connecting to config database ...\n";

my $dbh = DBI->connect(
    "dbi:SQLite:dbname=" . $config_dir . "/broadway_proxy_config.db"
  , undef # username
  , undef # password
  , {}    # options hash
) || die( DBI->errstr );

# Create our 'simple_config' table if it doesn't exist - this is required for our upgrade logic
$dbh->do(
    "create table if not exists simple_config (\n"
  . "    key    text       primary key\n"
  . "  , value  text\n"
  . ")"
) || die( $dbh->errstr );

sub do_upgrades {
    # $options will contain:
    # {
    #     current_version => $some_version_integer
    #   , upgrade_path    => $some_path_that_contains_schema_upgrades
    # }
    #
    # This sub performs upgrades to our database schema by parsing filenames in a given directory
    # ( filenames must contain a sequence number ). We've been passed our current version ( above ).
    # Any files with a sequence higher than our current version number are executed, in order, and we
    # then update the current version ( which is stored in simple_config ).
    my $options = shift;
    {
        no warnings 'uninitialized';
        print "Checking for upgrades ... current schema version: [" . $options->{current_version} . "]\n";
    }
    my $upgrade_hash = {};
    if ( ! -d $options->{upgrade_path} ) {
        return;
    }
    opendir( DIR, $options->{upgrade_path} ) || warn( $! );
    while ( my $file = readdir(DIR) ) {
        if ( $file =~ /(\d*)_([\w-]*)\.(dml|ddl)$/i ) {
            my ( $sequence, $name, $extension ) = ( $1, $2 );
            $upgrade_hash->{$sequence} = $file;
        }
    }
    close DIR;
    foreach my $sequence ( sort { $a <=> $b } keys %{$upgrade_hash} ) {
        if ( ! defined $options->{current_version} || $sequence > $options->{current_version} ) {
            my $this_file = $options->{upgrade_path} . "/" . $upgrade_hash->{$sequence};
            local $/;
            my $this_fh;
            open ( $this_fh, "<$this_file" )
                || die( $! );
            my $contents = <$this_fh>;
            close $this_fh;
            print "Executing schema upgrade: [" . $upgrade_hash->{$sequence} . "]\n";
            $dbh->do( $contents )
                || die( "Error upgrading schema:\n" . $dbh->errstr );
            # Bump the schema version. Unfortunately we don't have upsert syntax yet ( v3.22.0 vs v3.28.0 )
            my $records = $dbh->do( qq{ update simple_config set value = ? where key = 'version' }
              , {}
              , ( $sequence )
            ) || die( $dbh->errstr );
            if ( $records eq PERL_ZERO_RECORDS_INSERTED ) {
                $dbh->do( qq{ insert into simple_config ( key , value ) values ( 'version' , ? ) }
                  , {}
                  , ( $sequence )
                ) || die( $dbh->errstr );
            }
        }
    }
}

# Get our version and upgrade path, and trigger schema upgrades
my $current_dir = cwd();
my $upgrade_path = $current_dir . "/schema_upgrades";
my $sth = $dbh->prepare( "select value from simple_config where key = 'version'" ) || die( $dbh->errstr );
$sth->execute() || die( $sth->errstr );
my $row = $sth->fetchrow_hashref();
my $current_version = $row->{value};

# For new setups, we always re-initalize the admin user
my $is_new_setup;
if ( ! defined $current_version ) {
    $is_new_setup = 1;
}

do_upgrades(
    {
        current_version => $current_version
      , upgrade_path    => $upgrade_path
    }
);

# We're ready to launch the config GUI ...
print "Schema Init / Upgrade done ...\n";

if ( $is_new_setup || $reset_admin_user ) {
    print "\nAn 'admin' user will now be created.\n";
    print "\nPlease provide a password for the 'admin' user:\n";
    ReadMode 2;
    my $pass_1 = <STDIN>;
    ReadMode 1;
    chomp( $pass_1 );
    print "Please *confirm* the password for the 'admin' user:\n";
    ReadMode 2;
    my $pass_2 = <STDIN>;
    chomp( $pass_2 );
    ReadMode 1;
    if ( $pass_1 eq $pass_2 ) {
        $dbh->do( "delete from users where username = 'admin'" )
          || die( $dbh->errstr );
        $sth = $dbh->prepare( "insert into users ( username , password ) values ( ? , ? )" )
          || die( $dbh->errstr );
        $sth->execute( 'admin' , md5_hex( $pass_1 ) )
          || die( $sth->errstr );
        $dbh->do( "delete from apps where app_name = 'admin'" )
          || die( $dbh->errstr );
        $dbh->do( "insert into apps ( app_name , app_command ) values ( 'admin' , 'perl $current_dir/config.pl' )" )
          || die( $dbh->errstr );
        $dbh->do( "delete from user_apps where username = 'admin' and app_name = 'admin'" )
          || die( $dbh->errstr );
        $dbh->do( "insert into user_apps ( username , app_name ) values ( 'admin' , 'admin' )" )
          || die( $dbh->errstr );
    }
    print "\nusername: [admin] configured with the password you just supplied ...\n";
    print "\nTo reset the admin password, run the 'init_upgrade_database.pl' script with the --reset-admin-user command-line arg.\n";
}
