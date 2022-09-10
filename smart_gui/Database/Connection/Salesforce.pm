package Database::Connection::Salesforce;

use parent 'Database::Connection';

use strict;
use warnings;

use WWW::Salesforce;
use WWW::Mechanize;
use JSON::XS;

use SQL::Statement;

use Glib qw | TRUE FALSE |;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION       => 'char2hexint';
use constant LENGTH_FUNCTION        => 'length';
use constant SUBSTR_FUNCTION        => 'substr';

use constant SALESFORCE_API_VERSION => '39.0';

use constant DB_TYPE                => 'Salesforce';

# Developer notes:
# Let it be known that Salesforce is fucked.
# They bump their API versions regularly, and somewhere between v20 and v38, they broke access to
# some sObject types that we need - but I didn't find that out until after I'd basically completed
# this driver, using WWW:Salesforce ( which is kinda abandonware and stuck at around v20 ).
#
# So, I've moved to using their REST API with WWW:Mechanize ... BUT ... we use WWW::Salesforce to
# do the initial OAuth2 login and fetch the session id. I'm not rewriting that unless I have to.

sub connection_label_map {
    
    my $self = shift;
    
    return {
        Username        => "Username"
      , Password        => "Password"
      , Host_IP         => ""
      , Database        => ""
      , Port            => ""
      , Attribute_1     => "Token"
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };
    
}

sub default_port {

    my $self = shift;

    return -1;

}

sub connect_do {
    
    my ( $self, $auth_hash, $options_hash ) = @_;
    
    eval {
        
        $self->{connection} = WWW::Salesforce->login(
            username => $auth_hash->{Username}
          , password => $auth_hash->{Password} . $auth_hash->{Attribute_1}
        );
        
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
    
    if ( $self->{connection}->{sf_serverurl} =~ /(https:\/\/[\w\d]*\.salesforce\.com)/ ) {
        $self->{sf_server} = $1;
    } else {
        $self->dialog(
            {
                title   => "Parse issue"
              , type    => "warning"
              , text    => "Failed to parse the salesforce server out of the sf_serverurl: [" . $self->{connection}->{sf_serverurl} . "].\n\n"
                         . "Defaulting to [https://na1.salesforce.com]"
            }
        );
        $self->{sf_server} = "https://na1.salesforce.com";
    }
    
    my $hdr = $self->{connection}->get_session_header();
    $self->{session_id} = ${$hdr->{_value}->[0]}->{_value}->[0];
    
    print "\n\nSession ID: " . $self->{session_id} . "\n\n";
    
    $self->{mech} = WWW::Mechanize->new();
    $self->{mech}->agent( 'Mozilla/5.0' );
    $self->{mech}->add_header( "Authorization" => "OAuth " . $self->{session_id} );
    $self->{mech}->add_header( "X-PrettyPrint" => '1' );
    
    return 1;
    
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table, $options ) = @_;

    # $options contains:
    #{
    #    dont_quote      => 0
    #}

    if ( $options->{dont_quote} ) {
        return $table;
    }else{
        return '"' . $table . '"'
    }
    
}

sub has_schemas {
    
    my $self = shift;
    
    return 0;
    
}

sub fetch_database_list {
    
    my $self = shift;
    
    carp( "Can't call Database::Connection::Salesforce::fetch_database_list() ... Salesforce doesn't have database containers" );
    
    return ( "dummy_database" );
    
}

sub fetch_table_list {
    
    my ( $self, $database, $schema ) = @_;

    $self->{mech}->get( $self->{sf_server} . "/services/data/v" . SALESFORCE_API_VERSION . "/sobjects/" );

    my $sobjects = decode_json( $self->{mech}->content )->{sobjects};

    my $tables;

    foreach my $sobject ( @{$sobjects} ) {
        push @{$tables}, $sobject->{name};
    }

    return sort @{$tables};
    
}

sub fetch_view_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return ();
    
}

sub fetch_function_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return ();
    
}
sub fetch_procedure_list {
    
    my ( $self, $database, $schema ) = @_;
    
    return ();
    
}

sub fetch_salesforce_table_desc {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    if ( ! exists $self->{schema_cache}->{ $table } ) {
        $self->{mech}->get( $self->{sf_server} . "/services/data/v" . SALESFORCE_API_VERSION . "/sobjects/" . $table . "/describe" );
        $self->{schema_cache}->{ $table } = decode_json( $self->{mech}->content );
    }
    
    return $self->{schema_cache}->{ $table };
    
}

sub fetch_column_info_array {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $sf_column_info = $self->fetch_salesforce_table_desc( $database, $schema, $table );
    
    my $return;
    
    # Split out the DATA_TYPE and PRECISION ...
    foreach my $field ( @{$sf_column_info->{fields}} ) {
        
        push @{$return}
      , {
              COLUMN_NAME     => $field->{name}
            , DATA_TYPE       => $field->{type}
            , PRECISION       => undef
            , NULLABLE        => ( $field->{nillable} eq 'false' ? 0 : 1 )
            , COLUMN_DEFAULT  => $field->{defaultValue}
        };
        
    }
    
    return $return;
    
}

sub fetch_field_list {
    
    my ( $self, $database, $schema, $table, $options ) = @_;
    
    my $sf_column_info = $self->fetch_salesforce_table_desc( $database, $schema, $table );
    
    my $return;
    
    foreach my $column ( @{$sf_column_info->{fields}} ) {
        push @{$return}, $column->{name};
    }
    
    return $return;
    
}

sub can_execute_ddl {
    
    my $self = shift;
    
    return FALSE;
    
}

sub is_sql_database {

    my $self;

    return FALSE;

}

sub can_alias {
    
    my $self = shift;
    
    return FALSE;
    
}

sub sql_to_sqlite {

    my ( $self, $sql, $progress_bar ) = @_;

    # This function pulls records from Salesforce and pushes them into a SQLite DB
    # Muhahahahahaha!

    my $sqlite_dbh = Database::Connection::SQLite->new(
        $self->{globals}
      , {
            location    => ":memory:"
        }
    );
    
    my $parser = SQL::Parser->new();
    $parser->{RaiseError} = 1;
    $parser->{PrintError} = 0;
    
    # Strip out filter before passing it to SQL::Statement, as Salesforce queries can have
    # brain-dead unquoted string literals, which fails to parse
    
    my $sane_sql = $sql;
    
    if ( $sql =~ /(.*)(where.*)/sig ) {
        $sane_sql = $1;
    }
    
    my $stmt = SQL::Statement->new( $sane_sql, $parser );
    
    my $column_names;
    
    foreach my $column_def ( @{$stmt->column_defs} ) {
        my $this_col = $column_def->{fullorg};
        $this_col =~ s/.*\.//; # strip out aliases
        push @{$column_names}, $this_col;
    }
    
    my $local_sql = "create table salesforce_table (\n    ";
    my ( @column_def_strings, @placeholders );
    
    foreach my $fieldname ( @{$column_names} ) {
        
        push @column_def_strings, $fieldname . " text";
        push @placeholders, "?";
        
    }
    
    $local_sql .= join( "\n  , ", @column_def_strings ) . "\n)";
    
    print "\n$local_sql\n";
    
    $sqlite_dbh->do( $local_sql );
    
    $sqlite_dbh->{AutoCommit} = 0;
    
    $local_sql = "insert into salesforce_table (\n    " . join( "\n  , ", @{$column_names} )
        . "\n) values (\n    " . join( "\n  , ", @placeholders ) . "\n)";
    
    my $insert_sth = $sqlite_dbh->prepare( $local_sql )
        || die( $sqlite_dbh->errstr );
    
    $progress_bar->set_text( "Executing Salesforce query ..." );
    
    Gtk3::main_iteration() while ( Gtk3::events_pending() );
    
    my $counter = 0;

    # We need to replace spaces with pluses ( + )
    my $soql = $sql;
    $soql =~ s/\s/\+/g;

    eval {

        my $nextRecordsUrl;

        while ( $soql || $nextRecordsUrl ) {

            my $these_results;

            if ( $soql ) {
                $self->{mech}->get( $self->{sf_server} . "/services/data/v" . SALESFORCE_API_VERSION . "/query/?q=$soql" );
                $these_results = decode_json( $self->{mech}->content );
                $soql = undef;
            } else {
                $self->{mech}->get( $nextRecordsUrl );
                $these_results = decode_json( $self->{mech}->content );
            }

            foreach my $record ( @{$these_results->{records}} ) {
                $counter ++;
                if ( $counter % 500 == 0 ) {
                    $sqlite_dbh->{AutoCommit} = 1;
                    if ( $progress_bar ) {
                        $progress_bar->set_text( $counter );
                        $progress_bar->pulse;
                        Gtk3::main_iteration while ( Gtk3::events_pending );
                    }
                    $sqlite_dbh->{AutoCommit} = 0;
                }
                my $row;
                foreach my $column ( @{$column_names} ) {
                    if ( ! exists $record->{$column} ) {
                        die( "Didn't see column $column in returned recordset. You might have case sensitivity issues ( our implementation is case sensitive )" );
                    }
                    my $this_value = $record->{$column};
                    if ( ref $this_value eq 'ARRAY' ) {
                        push @{$row}, $record->{$column}->[0]; # TODO: wtf are these arrays?
                    } else {
                        push @{$row}, $record->{$column};
                    }
                }
                $insert_sth->execute( @{$row} )
                    || confess( $insert_sth->errstr );
            }

            if ( $these_results->{nextRecordsUrl} ) {
                $nextRecordsUrl = $these_results->{nextRecordsUrl};
            }

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
    
    return ( $sqlite_dbh, "select * from salesforce_table" );
    
}

1;
