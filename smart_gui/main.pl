#!/usr/bin/perl -w

use strict;
use warnings;

use 5.20.0;

use DBI;
use Cwd;
use Getopt::Long;
#use XML::Simple;
use File::Copy;

use Gtk3 -init;
use Glib qw/TRUE FALSE/;

use Gtk3::Ex::DBI::Form;
use Gtk3::Ex::DBI::Datasheet;

use Database::ConfigManager;
use Database::ConfigManager::SQLite;

use Database::Connection;
use Database::Connection::SQLite;       # we need to load this one very early on, before dynamic loading is workable

use widget::conn_db_table_chooser;

#use window::data_loader::Connection;

use window;

my @command_line_args = @_;

my $currentdir = cwd;
my $appdir = "$currentdir/";
my $parentdir;
my $logdir;

$ENV{'PGAPPNAME'} = 'Smart Data Framework';

my ( $open, $user_profile, $gui_overlay_path
   , $skip_upgrades , $flatpak , $no_redirect ); # stores values of options passed in from command line

my $wm_class = 'Smart Data Framework';

GetOptions(
    'open=s'             => \$open
  , 'user-profile=s'     => \$user_profile
  , 'gui-overlay-path=s' => \$gui_overlay_path
  , 'skip-upgrades'      => \$skip_upgrades
  , 'wm-class'           => \$wm_class
  , 'flatpak'            => \$flatpak
  , 'no-redirect'        => \$no_redirect
);

if ( ! $ENV{'SMART_CONFIG_BASE'} ) {
    if ( $^O eq "linux" ) {
        if ( $ENV{'XDG_CONFIG_HOME'} ) {
            $ENV{'SMART_CONFIG_BASE'} = $ENV{'XDG_CONFIG_HOME'} . "/profiles";
        } else {
            $ENV{'SMART_CONFIG_BASE'} = $ENV{"HOME"} . "/.smart_config";
        }
#        if ( $ENV{'XDG_DATA_HOME'} ) {
        if ( $flatpak ) {
            $logdir = $ENV{"HOME"} . "/SDF_persisted/logs";
        }
    } else {
        die( "Missing SMART_CONFIG_BASE environment variable - you MUST set this for non-linux installations" );
    }
}

if ( ! -d $ENV{'SMART_CONFIG_BASE'} ) {
    mkdir( $ENV{'SMART_CONFIG_BASE'} )
        || die( "Failed to create profiles directory [" . $ENV{'SMART_CONFIG_BASE'} . "]:\n" . $! );
}

if ( ! $logdir ) {
    $logdir = "/tmp/sdf_logs";
}

print( "\nSMART_CONFIG_BASE:       [" . $ENV{'SMART_CONFIG_BASE'} . "]\n" );
print( "Log directory:           [$logdir]\n" );

mkdir( $logdir );

if ( $currentdir =~ /(.*)\/.*/ ) {
    $parentdir = $1;
}

if ( ! $user_profile ) {
    if ( $ENV{'SDF_USER_PROFILE'} ) {
        $user_profile = $ENV{'SDF_USER_PROFILE'};
    } else {
        $ENV{'SDF_USER_PROFILE'} = getpwuid($<);
        $user_profile = $ENV{'SDF_USER_PROFILE'};
    }
}

my $user_profile_path       = $ENV{'SMART_CONFIG_BASE'} . "/" . $user_profile;

print "Using user profile path: [$user_profile]\n";

if ( ! -d $user_profile_path ) {
    mkdir( $user_profile_path )
        || die( "Failed to create user profile path [$user_profile_path]:\n" . $! );
}

my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
my $timestamp = sprintf( "%04d%02d%02d_%02d%02d%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );

# Redirect STDOUT / STDERR to log files
my $app_log_path = "$logdir/app_" . $timestamp . ".log";
my $err_log_path = "$logdir/err_" . $timestamp . ".log";

my ( $STDOUT_READER , $STDERR_READER );

if ( ! $no_redirect ) {

    say( "Redirecting:\nSTDOUT: $app_log_path\nSTDERR: $err_log_path" );
    open STDOUT, '>', $app_log_path or die "Can't redirect STDOUT: $!";
    open STDERR, '>', $err_log_path or die "Can't redirect STDERR: $!";

    # open $STDOUT_READER , "<" , $app_log_path or die( "Can't open stdout log file!: $!" );
    open( $STDOUT_READER, "tail -f $app_log_path |" )
        || die( "Can't fork!\n" . $! );

    # open $STDERR_READER , "<" , $err_log_path or die( "Can't open stderr log file!: $!" );
    open( $STDERR_READER, "tail -f $err_log_path |" )
        || die( "Can't fork!\n" . $! );

}

# Turn off output buffering.
# This is so stuff gets dumped to the console / app log immediately instead of somewhat after the event.
select STDERR;
$| = 1;

select STDOUT;
$| = 1;

print "Running under:           [" . $^O . "]\n";

if ( $ENV{'GDK_BACKEND'} && $ENV{'GDK_BACKEND'} eq 'broadway' ) {
    print( "Running under:     broadway\n" );
}

if ( $^O eq "linux" ) {
    unlink "/tmp/sql.log";                                  # Delete FreeTDS / unixODBC log, which gets HUGE!
}

print "\n\n\@INC paths ...\n";

use Cwd 'abs_path';                 # Get the full path of ourself in the next line
my $self_name = abs_path( $0 );     # $0 is the full path to this file

foreach my $i( @INC ) { print "$i\n"; }

if ( $flatpak ) {
    print "Running inside flatpak\n";
    print "Copying /app/etc/odbcinst.ini to ~/.odbcinst.ini\n";
    copy( "/app/etc/odbcinst.ini", $ENV{"HOME"} . "/.odbcinst.ini" )
        || die( "Copy failed!\n" . $! );
}

my $globals = {
    self    => {
        path                => $self_name
      , command_line_args   => \@command_line_args
      , user_profile        => $user_profile
      , wm_class            => $wm_class
      , flatpak             => $flatpak
    }
  , paths   => {
          app              => $appdir
        , parent           => $parentdir
        , profile          => $user_profile_path
        , builder          => $appdir . "/builder"
        , reports          => $appdir . "/reports/output"
        , home             => $ENV{HOME}
        , logs             => $logdir
        , app_log_path     => $app_log_path
        , err_log_path     => $err_log_path
        , image_path_cache => { }
    }
  , windows                 => { }
  , behaviour               => { }
  , STDOUT_READER          => $STDOUT_READER
  , STDERR_READER          => $STDERR_READER
};

if ( $skip_upgrades ) {
    $globals->{behaviour}->{skip_upgrades} = 1;
}

#####################################################################################################################################
# Connect to Databases
#
# Local configuration database. This resides in the user's profile path
# and is NOT distributed. It gets auto-generated here if it doesn't exist.

eval {
    
    $globals->{local_db} = Database::Connection::SQLite->new(
        $globals
      , {
            location    => $user_profile_path . "/config.db"
            #location    => ":memory:"
        }
      , 0
      , 0
      , {
            setup_include_paths => 1
        }
    ) || die "I can't make a connection to the configuration ( SQLite ) database!\n"
           . "Please check that you have DBD::SQLite drivers installed\n"
           . "and that you have read/write access in the 'smart_gui' directory";
    
    $globals->{config_manager} = Database::ConfigManager::SQLite->new( $globals, $globals->{local_db}, "config" );
    
    # First, inject any new overlay path from the command-line ...
    # Overlays allow 3rd parties to add components to the GUI while keeping a clear
    # separation of code and objects. The command-line option is here basically so we can
    # push out installations that have plugins that are 'automatically' added, as opposed
    # to a user/admin having to manually open the configuration screen and add the overlay
    
    if ( $gui_overlay_path ) {
        my $overlay_name;
        if ( $gui_overlay_path =~ /(\w*)=([\/\\\w]*)/ ) {
            ( $overlay_name, $gui_overlay_path ) = ( $1, $2 );
        } else {
            $overlay_name = $gui_overlay_path;
        }
        
        $globals->{local_db}->do(
            "insert into gui_overlays ( OverlayName , OverlayPath ) values ( ? , ? )"
          , [ $overlay_name , $gui_overlay_path ]
        );
    }
    
};

if ( $@ ) {
    window::dialog(
        undef
      , {
            title   => "Error creating local configuration database!",
            type    => "error",
            text    => $@
        }
    );
    exit;
}

my $sdf_db_prefix = $globals->{config_manager}->simpleGet( "SDF_DB_PREFIX" );

$globals->{CONTROL_DB_NAME} = $sdf_db_prefix . "_CONTROL";
$globals->{LOG_DB_NAME}     = $sdf_db_prefix . "_LOG";

$Gtk3::Ex::DBI::USE_COMPAT_FILTER_CLAUSE = 1; # We need to set this for now, as DBD::Proxy doesn't expose the server name ...

# Load some custom CSS
my $provider = Gtk3::CssProvider->new();
$provider->load_from_path( "custom.css" );
my $display = Gtk3::Gdk::Display::get_default();
my $screen = $display->get_default_screen;

Gtk3::StyleContext::add_provider_for_screen ( $screen , $provider , Gtk3::STYLE_PROVIDER_PRIORITY_USER );

{
    
    my $first_run_complete = $globals->{config_manager}->simpleGet( "FIRST_RUN_COMPLETE" );
    
    if ( ! $first_run_complete ) {
    
        $open = '1st_run_wizard';
        
        window::dialog(
            undef
          , {
                title       => "Welcome to Smart Data Frameworks!"
              , type        => "info"
              , text        => "This is probably the 1st time you've launched SDF's GUI.\n\n"
                             . "The 1st run wizard will be launched. Step through each page of the wizard,"
                             . " and click the 'Complete Wizard' button on the last page to indicate your"
                             . " configuration is complete."
            }
        );
        
    }
    
    if ( ! $open ) {
        $open = 'browser';
    }
    
    # Gtk3::SourceView is only going to work on Linux at the moment ...
    # TODO: It's getting less likely we'll support this on anything other than Linux,
    #       now that flatpak is here. Remove if():
    if ( $^O eq "linux" ) {
        
        use Gtk3::SourceView;
        
        # Set up our language ( SQL + tokens ) specs for syntax highlighting in GtkSourceView widgets
        $globals->{gtksourceview_language_manager} = Gtk3::SourceView::LanguageManager->get_default();
        my @language_spec_locs = $globals->{gtksourceview_language_manager}->get_search_path;
        push @language_spec_locs, $globals->{paths}->{app} . "/gtksourceview/language-specs";
        $globals->{gtksourceview_language_manager}->set_search_path( \@language_spec_locs );
        $globals->{gtksourceview_language} = $globals->{gtksourceview_language_manager}->get_language( 'smartsql' );
        
        # TODO: UI to list / set style
        $globals->{gtksourceview_style_schema_manager} = Gtk3::SourceView::StyleSchemeManager->get_default();
        #my @schemes = $ssm->get_scheme_ids;
        $globals->{gtksourceview_scheme} = $globals->{gtksourceview_style_schema_manager}->get_scheme( 'classic' );
        # $globals->{gtksourceview_scheme} = $globals->{gtksourceview_style_schema_manager}->get_scheme( 'solarized-dark' );
    }
    
    my $startup_window_class = 'window::' . $open;
    
    window::open_window( undef, $startup_window_class, $globals );
    
    if ( $startup_window_class ne 'window::1st_run_wizard' ) {
        # Try to connect to the config DB. If this fails, don't bother opening the release manager - it won't work
        my $dbh = $globals->{config_manager}->sdf_connection( "CONTROL" );
        if ( $dbh ) {
            window::open_window( undef , 'window::release_manager' , $globals , { close_after_update_check => 1 } );
        }
    }
    
    Gtk3->main;
    
}
