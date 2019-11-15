package Database::Connection::SQLite;

use strict;
use warnings;

use 5.20.0;

use parent 'Database::Connection';

use constant DB_TYPE            => 'SQLite';

sub new {
    
    my ( $class, $globals, $auth_hash, $dont_connect, $progress_bar, $options_hash ) = @_;
    
    my $self = shift->SUPER::new( $globals, $auth_hash, $dont_connect, $progress_bar, $options_hash );
    
    if ( ! $self ) {
        return undef;
    }
    
    if ( $dont_connect ) {
        return $self;
    }
    
    # Synchronous mode is slow ... turn it off
    $self->do( "PRAGMA default_synchronous = OFF" );
    
    #  ... and make the journal in-memory
    $self->do( "PRAGMA journal_mode = MEMORY" );

    if ( $options_hash->{setup_include_paths} ) {
        my @include_paths = $self->all_gui_paths;
        foreach my $path ( @include_paths ) {
            push @INC, $path;
        }
    }

    return $self;
    
}

sub connection_label_map {

    my $self = shift;

    return {
        Username        => ""
      , Password        => ""
      , Database        => ""
      , Host_IP         => "Path to SQLite file"
      , Port            => ""
      , Attribute_1     => ""
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

sub connect_pre {
    
    my ( $self , $auth_hash , $options_hash ) = @_;
    
    if ( ! $auth_hash->{ConnectionString} || $auth_hash->{Host} ) {
        $auth_hash->{ConnectionString} = $self->build_connection_string( $auth_hash );
    }
    
    return ( $auth_hash , $options_hash );
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    my $location = $auth_hash->{location} || $auth_hash->{Host};
    
    if ( ! $location ) {
        carp( "Missing 'location' and 'Host' key" );
    }
    
    no warnings 'uninitialized';
    
    my $string = "dbi:SQLite:dbname=" . $location;

    say( "Creating SQLite connection at: [$string]" );

    return $self->SUPER::build_connection_string( $auth_hash, $string );
    
}

sub fetch_database_list {
    
    my $self = shift;
    
    print( "Can't call Database::Connection::SQLite::fetch_database_list() ... SQLite doesn't have database containers" );
    
    return ( "dummy_database" );
    
}

sub has_schemas {
    
    my $self = shift;
    
    return 0;
    
}

sub fetch_table_list {
    
    my ( $self, $database ) = @_;
    
    my $sth = $self->prepare(
        "select name from sqlite_master where type='table'"
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my @return;
    
    while ( my $row = $sth->fetchrow_arrayref ) {
        push @return, $$row[0];
    }
    
    return sort( @return );
    
}

sub fetch_view_list {
    
    my $self = shift;
    
    return ();
    
}

sub fetch_function_list {
    
    my $self = shift;
    
    return ();
    
}

sub fetch_procedure_list {
    
    my $self = shift;
    
    return ();
    
}

sub fetch_column_info {
    
    my ( $self, $database, $schema, $table ) = @_;
    
    my $sth = $self->prepare(
        "pragma table_info( '$table' )"
    ) || return;
    
    $self->execute( $sth )
        || return;
    
    my $column_info = $sth->fetchall_hashref( "name" );
    my $return;
    
    foreach my $key ( keys %{$column_info} ) {
        $return->{$key} = {
            COLUMN_NAME     => $key
          , DATA_TYPE       => $column_info->{$key}->{type}
          , NULLABLE        => ! $column_info->{$key}->{notnull}
          , COLUMN_DEFAULT  => $column_info->{$key}->{dflt_value}
        };
    }
    
    return $return;
    
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

# NP Creates the local sqlite memdb tables for storing metadata, used in target DDL creation, treeviews etc
# TODO maybe define these in external sql files at some point and slurp in?
sub create_model_schema {
    
    my $self = shift;
    
    # This method creates all the tables we use to model a database's schema
    
    $self->do(
        "create table tables (\n"
      . "    ID                    integer       primary key\n"
      . "  , table_type            text\n"
      . "  , source_database_name  text\n"
      . "  , source_schema_name    text\n"
      . "  , source_table_name     text\n"
      . "  , database_name         text\n"
      . "  , schema_name           text\n"
      . "  , table_name            text\n"
      . "  , val_database_name     text\n"
      . "  , val_schema_name       text\n"
      . "  , val_table_name        text\n"
      . "  , include               integer\n"
      . "  , issues                integer\n"
      . "  , ddl                   text\n"
      . "  , executed              integer\n"
      . "  , error_message         text\n"
      . "  , warning               text\n"
      . "  , dont_autogenerate     integer       not null default 0\n"
      . "  , notes                 text"
      . ")" ) || return;
    
    $self->do(
        "create index IDX_DB_SCHEMA_TABLE on tables (database_name,schema_name,table_name)"
    ) || return;
    
    $self->do(
        "create table views (\n"
      . "    ID                    integer       primary key\n"
      . "  , source_database_name  text\n"
      . "  , source_schema_name    text\n"
      . "  , source_view_name      text\n"
      . "  , database_name         text\n"
      . "  , schema_name           text\n"
      . "  , view_name             text\n"
      . "  , view_definition       text\n"
      . "  , include               integer       default 1\n"
      . "  , ddl                   text\n"
      . "  , executed              text\n"
      . "  , error_message         text\n"
      . "  , warning               text\n"
      . "  , dont_autogenerate     integer       not null default 0"
      . "  , notes                 text\n"
      . ")" ) || return;
    
    $self->do(
        "create table table_columns (\n"
      . "    ID                    integer       primary key\n"
      . "  , database_name         text\n"
      . "  , schema_name           text\n"
      . "  , table_name            text\n"
      . "  , column_name           text\n"
      . "  , column_type           text\n"
      . "  , column_precision      text\n"
      . "  , column_nullable       integer\n"
      . "  , column_default        text\n"
      . "  , target_column_type    text\n"
      . "  , target_column_default text\n"
      . "  , notes                 text\n"
      . ")" ) || return;
    
    $self->do(
        "create table indexes (\n"
      . "    ID                    integer       primary key\n"
      . "  , database_name         text\n"
      . "  , schema_name           text\n"
      . "  , table_name            text\n"
      . "  , index_name            text\n"
      . "  , is_primary            integer\n"
      . "  , is_unique             integer\n"
      . "  , is_distribution_key   integer\n"
      . "  , is_organisation_key   integer\n"
      . "  , include               integer       not null default 1\n"
      . "  , ddl                   text\n"
      . "  , executed              integer\n"
      . "  , error_message         text\n"
      . "  , warning               text\n"
      . "  , dont_autogenerate     integer       not null default 0"
      . "  , notes                 text\n"
      . ")" ) || return;
    
    $self->do(
        "create table index_columns (\n"
      . "    ID                    integer       primary key\n"
      . "  , index_ID              integer\n"
      . "  , column_name           text\n"
      . ")" ) || return;
    
    $self->do(
        "create table fk_rels (\n"
      . "    ID                    integer       primary key\n"
      . "  , relationship_name     text\n"
      . "  , primary_database      text\n"
      . "  , primary_schema        text\n"
      . "  , primary_table         text\n"
      . "  , foreign_database      text\n"
      . "  , foreign_schema        text\n"
      . "  , foreign_table         text\n"
      . "  , table_ID              integer\n"
      . "  , ddl                   text\n"
      . "  , include               integer       not null default 1\n"
      . "  , executed              integer\n"
      . "  , error_message         text\n"
      . "  , warning               text\n"
      . "  , dont_autogenerate     integer       not null default 0"
      . "  , notes                 text\n"
      . ")" ) || return;
    
    $self->do(
        "create table fk_rel_parts (\n"
      . "    ID                    integer       primary key\n"
      . "  , fk_rel_ID             integer\n"
      . "  , primary_column        text\n"
      . "  , foreign_column        text\n"
      . ")" ) || return;

    $self->do(
        "create table sequences (\n"
      . "    id                    integer       primary key\n"
      . "  , database_name         text\n"
      . "  , schema_name           text\n"
      . "  , table_name            text\n"
      . "  , sequence_name         text\n"
      . "  , sequence_type         text\n"
      . "  , target_sequence_type  text\n"
      . "  , sequence_increment    integer\n"
      . "  , sequence_min          integer\n"
      . "  , sequence_max          integer\n"
      . "  , extra_options         text\n"
      . "  , note                  text\n"
      . "  , ddl                   text\n"
      . "  , include               integer       not null default 1\n"
      . "  , executed              integer\n"
      . "  , error_message         text\n"
      . "  , warning               text\n"
      . "  , dont_autogenerate     integer       not null default 0"
      . "  , notes                 text"
      . ")" ) || return;

    # NP All of these statements are prepared here for execution in '_model_fetch_and_populate_*' methods
    $self->{_model_index_insert_sth} = $self->prepare(
        "insert into indexes\n"
      . "(\n"
      . "    database_name , schema_name , table_name , index_name , is_primary , is_unique, is_distribution_key , is_organisation_key\n"
      . ") values ( ? , ? , ? , ? , ? , ?, ? , ? )"
    );
    
    $self->{_model_index_columns_insert_sth} = $self->prepare(
        "insert into index_columns\n"
      . "(\n"
      . "    index_ID , column_name\n"
      . ") values ( ? , ? )"
    );
    
    $self->{_model_fk_rel_insert_sth} = $self->prepare(
        "insert into fk_rels\n"
      . "(\n"
      . "    relationship_name , primary_database , primary_schema , primary_table , foreign_database , foreign_schema , foreign_table\n"
      . ") values ( ? , ? , ? , ? , ? , ? , ? )"
    );
    
    $self->{_model_fk_rel_part_insert_sth} = $self->prepare(
        "insert into fk_rel_parts\n"
      . "(\n"
      . "    fk_rel_ID , primary_column , foreign_column\n"
      . ") values ( ? , ? , ? )"
    );
    
    $self->{_model_view_insert_sth} = $self->prepare(
        "insert into views\n"
      . "(\n"
      . "    source_database_name , source_schema_name , source_view_name , database_name , schema_name , view_name , view_definition\n"
      . ") values ( ? , ? , ? , ? , ? , ? , ? )"
    );

    $self->{_model_sequence_insert_sth} = $self->prepare(
        "insert into sequences\n"
      . "(\n"
      . "    database_name , schema_name , table_name , sequence_name , sequence_type , target_sequence_type , sequence_increment , sequence_min , sequence_max , extra_options , note\n"
      . ") values ( ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? )"
    );
}

sub gui_overlay_paths {
    
    my $self = shift;
    
    $self->{globals}->{suppress_error_dialogs} = 1;
    
    my $overlay_paths = $self->select(
        "select OverlayPath from gui_overlays where Active = 1"
    );
    
    $self->{globals}->{suppress_error_dialogs} = 0;
    
    my @return;
    
    foreach my $path ( @{$overlay_paths} ) {
        push @return, $path->{OverlayPath};
    }
    
    return @return;
    
}

sub all_gui_paths {
    
    my $self = shift;
    
    my @all_paths;
    
    push @all_paths, $self->{globals}->{paths}->{app};
    
    my @overlay_paths = $self->gui_overlay_paths;
    
    if ( @overlay_paths ) {
        push @all_paths, @overlay_paths;
    }
    
    return @all_paths;
    
}

sub all_odbc_config_paths {

    my $self = shift;

    my $odbc_config_paths = $self->select(
        "select Path from odbcinst_paths"
    );

    my @paths;

    foreach my $path ( @{$odbc_config_paths} ) {
        push @paths, $path->{Path};
    }

    return \@paths;

}

sub all_odbc_drivers {

    my $self = shift;

    my $odbc_drivers = $self->select(
        "select Driver from odbc_drivers"
    );

    my @drivers;

    foreach my $driver ( @{$odbc_drivers} ) {
        push @drivers, $driver->{Driver};
    }

    return \@drivers;

}

sub connection_browse_title {

    my $self = shift;

    return "Select a SQLite database file";

}

# NP To quickly show table, index etc information stored in memdb table to assist debugging.. There might be similar routines somewhere else...
sub _get_all_memdb_tables {
    my $self = shift;
    my $tables = $self->select("select name from sqlite_master where type = 'table'");
    
    return $tables;
}

sub _save_mem_db_tables_to_csv {
    my $self = shift;
    my $tbl_list = shift;

    foreach my $tbl ( @{ $tbl_list }) {
        my $name = "$tbl->{name}";
        my $filename = $ENV{HOME}."/mem_db_tbl_$name.csv";
        open my $file, ">$filename" or warn "Couldn't create csv for $name";
        print $file $self->_get_memdb_table_info ($name, undef);
        close ($file) or warn "Couldn't close csv for $name";;
    }
}

sub _print_memdb_tables {
    my ($self, $tbl_list) = @_;
    foreach my $tbl ( @{ $tbl_list }) {
        print "Table: \"$tbl->{name}\"\n\n";
        $self->_print_memdb_table( $tbl->{name} );
        print "\n\n";
    }
}

sub _print_memdb_table {
    my $self = shift;
    print $self->_get_memdb_table_info (@_);
}

sub _get_memdb_table_info {
    my ($self, $table, $query) = @_;
    # TODO implement the query thing later
    my $ctr=1;
    my $txt = "";
    my $delim="\t";
    foreach my $rec (@{ $self->select ("SELECT * FROM $table;") }) {
        # only print headers first time
        if ($ctr==1) {
            $txt .= join $delim, sort keys %{ $rec };
            $txt .= "\n";
        } else {
            my @cols=();
            foreach my $col (sort keys %{ $rec }) {
                my $tmp = $rec->{$col} || "NULL";
                $tmp=~s/(\n|\s)+/ /g;
                $tmp="\"$tmp\"";
                push @cols, $tmp;
            }
            $txt .= join $delim, @cols;
            $txt .= "\n";
        }
        $ctr++;
    }
    $txt;
}
1;
