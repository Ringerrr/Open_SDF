package SmartAssociates::Resolver::Base;

use strict;
use warnings;

use Carp;

use base 'SmartAssociates::Base';

my $IDX_RESOLVER_NAME                           =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 0;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 1;

sub new {

    my $self = $_[0]->SUPER::new(    $_[1] );

    $self->[ $IDX_RESOLVER_NAME ]  = $_[2];

    return $self;

}

sub split_comma_separated_columns {

    my ( $self, $modifier , $columns_string, $no_case_mangling ) = @_;

    # This method gets a comma-separated list of columns and returns them
    # as an array. It strips leading and trailing spaces, and forces
    # column names to UPPER case

    my @columns = split /,/, $columns_string;

    foreach my $column ( @columns ) {
        $column =~ s/\s//g;     # strip spaces
        if ( ! $no_case_mangling ) {
            $column = uc( $column ); # upper-case
        }
    }

    return @columns;

}

sub COMPLEX_COLUMNS_FROM_SOURCE {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $source_db     = $template_config->template_record->{SOURCE_DB_NAME};
    my $source_schema = $template_config->template_record->{SOURCE_SCHEMA_NAME};
    my $source_table  = $template_config->template_record->{SOURCE_TABLE_NAME};

    $source_db        = $template_config->detokenize( $source_db );
    $source_schema    = $template_config->detokenize( $source_schema );
    $source_table     = $template_config->detokenize( $source_table );

    my $column_names  = $template_config->target_database->get_fields_from_table(
        $source_db
      , $source_schema
      , $source_table
    );

    if ( exists $parameters->{'#P_IGNORE_SOURCE_COLS#'} ) {

        # We want to filter all items we've been told to ignore out of the column list we just fetched
        my @ignore_cols = split( ',', $parameters->{'#P_IGNORE_SOURCE_COLS#'}->{PARAM_VALUE} );
        my %ignore_keys;

        if ( @ignore_cols ) {
            %ignore_keys = @ignore_cols;
        } else {
            $self->log->warn( "#P_IGNORE_SOURCE_COLS# param exists, but is empty!" );
        }

        my $filtered_list;

        foreach my $item (@{$column_names}) {
            if ( ! exists $ignore_keys{ uc( $item ) } ) { # force to upper case
                push @{$filtered_list}, $item;
            }
        }

        $column_names = $filtered_list;

    }

    # Now we're about to start building more complex select SQL, so we're no longer dealing with column 'names'
    # but we need to keep things stored against this column name, AND in order. So we convert to a 2D array

    my @columns;

    foreach my $column ( @{$column_names} ) {
        push @columns, [ uc($column), $column ]; # uc() because we need to match case of field metadata
    }

    my @final_columns;

    # SKIP_SOURCE_FORMATTING will just return the list of column names, without applying
    # our formatting rules ( in the DB class' formatted_select() method ), which are used during
    # DB migrations

    if ( $modifier !~ /SKIP_SOURCE_FORMATTING/ ) {

        my $usage;
        if ( $modifier =~ /MIGRATION_SELECT/ ) {
            $usage = 'migration_select';
        } elsif ( $modifier =~ /MIGRATION_IMPORT/ ) {
            $usage = 'migration_import';
        } else {
            $self->log->warn( "#COMPLEX_COLUMNS_FROM_SOURCE# token used without a usage modifier. Defaulting to MIGRATION_SELECT ..." );
            $usage = 'migration_select';
        }

        # TODO - we need to copy @{$column_names} to @final_columns if this is set
        foreach my $column_array ( @columns ) {

            # Now apply formatting & manipulations from the database
            my $column_info = $template_config->target_database->fetch_column_type_info(
                $template_config->detokenize( $template_config->template_record->{SOURCE_DB_NAME} )
              , $template_config->detokenize( $template_config->template_record->{SOURCE_SCHEMA_NAME} )
              , $template_config->detokenize( $template_config->template_record->{SOURCE_TABLE_NAME} )
              , $$column_array[1]
              , $usage
            );

            my $this_expression = $column_info->{formatted_select};

            # Do we need to alias the expression back to the original column name?
            my $original_column_name = $$column_array[1];

            if (   $this_expression ne $$column_array[1]
                && $this_expression !~ /.*\sas\s$original_column_name$/       # don't add > 1 alias
                && $usage ne 'migration_import'                               # don't add alias for import expresssions
            ) {
                $this_expression .= " as " . $$column_array[1];
            }

            if ( $modifier =~ /QUOTE_COLUMNS/ ) {
                $this_expression = $template_config->target_database->quote_column_in_expression( $original_column_name , $this_expression );
            }

            push @final_columns, $this_expression;

        }

    } else {

        @final_columns = @{$column_names};

        if ( $modifier =~ /QUOTE_COLUMNS/ ) {
            foreach my $col ( @final_columns ) {
                $col = $template_config->target_database->quote_column_in_expression( $col , $col );
            }
        }

    }

    if ( $modifier =~ /WANT_ARRAY/ ) {
        return @final_columns;
    } else {
        return join(
            "\n  , "
          , @final_columns
        );
    }

}

sub COMPLEX_SOURCE_PREFIXED_COLUMNS_FROM_TARGET {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $target_db      = $template_config->template_record->{TARGET_DB_NAME};
    my $target_schema  = $template_config->template_record->{TARGET_SCHEMA_NAME};
    my $target_table   = $template_config->template_record->{TARGET_TABLE_NAME};

    $target_db         = $template_config->detokenize( $target_db );
    $target_schema     = $template_config->detokenize( $target_schema );
    $target_table      = $template_config->detokenize( $target_table );

    my $fields         = $template_config->target_database->get_fields_from_table( $target_db, $target_schema, $target_table );

    my $ignore_columns = $template_config->detokenize( '#P_ZZ_IGNORE_COLUMNS#' );
    my @ignore_columns;

    {
        no warnings 'uninitialized';
        @ignore_columns = split( ',', $ignore_columns );
    }

    foreach my $item ( @ignore_columns ) {
        $item =~ s/^\s+|\s+$//g;
    }

    my @final_columns;

    foreach my $field ( @{$fields} ) {

        if ( grep { $_ eq $field } @ignore_columns ) {
            next; # next item in foreach
        }

        push @final_columns, 'SOURCE.' . $field;

    }

    my $value = join(
        "\n  , "
        , @final_columns
    );

    return $value;

}

sub COMPLEX_COLUMNS_FROM_TARGET {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $target_db      = $template_config->template_record->{TARGET_DB_NAME};
    my $target_schema  = $template_config->template_record->{TARGET_SCHEMA_NAME};
    my $target_table   = $template_config->template_record->{TARGET_TABLE_NAME};

    $target_db         = $template_config->detokenize( $target_db );
    $target_schema     = $template_config->detokenize( $target_schema );
    $target_table      = $template_config->detokenize( $target_table );

    my $fields;

    $fields            = $template_config->target_database->get_fields_from_table( $target_db, $target_schema, $target_table );

    my $ignore_columns = $template_config->detokenize( '#P_ZZ_IGNORE_COLUMNS#' );
    my @ignore_columns;

    {
        no warnings 'uninitialized';
        @ignore_columns = split( ',' , $ignore_columns );
    }

    foreach my $item ( @ignore_columns ) {
        $item =~ s/^\s+|\s+$//g;
    }

    my @final_columns;

    foreach my $field ( @{$fields} ) {

        if ( grep { $_ eq $field } @ignore_columns ) {
            next; # next item in foreach
        }

        if ( $modifier =~ /QUOTE_COLUMNS/ ) {
            $field = $template_config->target_database->quote_column_in_expression( $field , $field );
        }

        push @final_columns, $field;

    }

    if ( $modifier =~ /WANT_ARRAY/ ) {
        return @final_columns;
    } else {
        return join(
            "\n  , "
          , @final_columns
        );
    }

}

sub COMPLEX_DOES_TABLE_EXIST {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $target_db     = $template_config->template_record->{TARGET_DB_NAME};
    my $target_schema = $template_config->template_record->{TARGET_SCHEMA_NAME};
    my $target_table  = $template_config->template_record->{TARGET_TABLE_NAME};

    $target_db        = $template_config->detokenize( $target_db );
    $target_schema    = $template_config->detokenize( $target_schema );
    $target_table     = $template_config->detokenize( $target_table );

    my $sql           = $template_config->target_database->does_table_exist_string(
        $target_db
      , $target_schema
      , $target_table
    );

    return $sql;

}

sub COMPLEX_DOES_DATABASE_NOT_EXIST {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $source_db     = $template_config->template_record->{SOURCE_DB_NAME};
    $source_db        = $template_config->detokenize( $source_db );;
    my $sql           = $template_config->target_database->does_database_not_exist_string( $source_db );

    return $sql;

}

sub COMPLEX_DOES_SCHEMA_NOT_EXIST {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $target_db     = $template_config->template_record->{TARGET_DB_NAME};
    my $target_schema = $template_config->template_record->{TARGET_SCHEMA_NAME};

    $target_db        = $template_config->detokenize( $target_db );
    $target_schema    = $template_config->detokenize( $target_schema );

    my $sql           = $template_config->target_database->does_schema_not_exist_string(
        $target_db
      , $target_schema
    );

    return $sql;

}

sub COMPLEX_CREATE_DATABASE {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $source_db     = $template_config->template_record->{SOURCE_DB_NAME};
    $source_db        = $template_config->detokenize( $source_db );
    my $sql           = $template_config->target_database->create_database_string( $source_db );

    return $sql;

}

sub COMPLEX_CREATE_SCHEMA {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $target_db     = $template_config->template_record->{TARGET_DB_NAME};
    my $target_schema = $template_config->template_record->{TARGET_SCHEMA_NAME};

    $target_db        = $template_config->detokenize( $target_db );
    $target_schema    = $template_config->detokenize( $target_schema );

    my $sql           = $template_config->target_database->create_schema_string(
        $target_db
      , $target_schema
    );

    return $sql;

}

sub COMPLEX_PRIMARY_KEYS_FROM_SOURCE {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $source_db     = $template_config->template_record->{SOURCE_DB_NAME};
    my $source_schema = $template_config->template_record->{SOURCE_SCHEMA_NAME};
    my $source_table  = $template_config->template_record->{SOURCE_TABLE_NAME};

    $source_db        = $template_config->detokenize( $source_db );
    $source_schema    = $template_config->detokenize( $source_schema );
    $source_table     = $template_config->detokenize( $source_table );

    my $pk_columns    = $template_config->target_database->get_primary_key_columns(
        $source_db
      , $source_schema
      , $source_table
    );

    my $value;

    if ( $pk_columns ) {
        $value = join( " , ", @{$pk_columns} );
    }

    return $value;

}

sub COMPLEX_COLUMNS_FROM_SOURCE_ENCRYPTED {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    # This token fetches a list of columns from the source, but encrypts any columns
    # listed in #P_ENCRYPT_COLUMNS#

    my $columns_string = $template_config->resolve_parameter( '#COMPLEX_COLUMNS_FROM_SOURCE#' );

    my @columns = split( /\r?\n/, $columns_string );

    my $encrypt_columns_string = $template_config->resolve_parameter( '#P_ENCRYPT_COLUMNS#' );

    my %encrypt_columns_hash = map { $_ => 1 } split /\r?\n/, $encrypt_columns_string;

    my @encrypted_columns;

    foreach my $column (@columns) {

        if ($encrypt_columns_hash{$column}) {
            push @encrypted_columns, $template_config->target_database->encrypt_expression( $column );
        } else {
            push @encrypted_columns, $column;
        }

    }

    my $value = join( ",", @encrypted_columns );

    return $value;

}

sub COMPLEX_CREATE_TMP_FILE {

    my ( $self, $modifier ,  $template_config, $parameters ) = @_;

    my ( $fh, $filename ) = tempfile(
        UNLINK => 0 # without this, the file will be deleted when $filename goes out of scope
    );

    return $filename;

}

sub COMPLEX_DB_SCHEMA_TABLE {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $db      = $template_config->detokenize( $template_config->template_record->{SOURCE_DB_NAME}     or $template_config->template_record->{TARGET_DB_NAME} );
    my $schema  = $template_config->detokenize( $template_config->template_record->{SOURCE_SCHEMA_NAME} or $template_config->template_record->{TARGET_SCHEMA_NAME} );
    my $table   = $template_config->detokenize( $template_config->template_record->{SOURCE_TABLE_NAME}  or $template_config->template_record->{TARGET_TABLE_NAME} );

    return $template_config->target_database->db_schema_table_string( $db, $schema, $table );

}

sub COMPLEX_TRUNCATE_DB_SCHEMA_TABLE {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $db      = $template_config->detokenize( $template_config->template_record->{TARGET_DB_NAME} );
    my $schema  = $template_config->detokenize( $template_config->template_record->{TARGET_SCHEMA_NAME} );
    my $table   = $template_config->detokenize( $template_config->template_record->{TARGET_TABLE_NAME} );

    return $template_config->target_database->truncate_db_schema_table_string( $db, $schema, $table );

}

sub COMPLEX_MYSQL_LOAD_DATA_SET_CLAUSE {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    return '';
    
    # This clause might look something like ( simplification ):
    # ( @col1 , @col2 , @col3 )
    # set
    # id = @col1 , description = @col2 , binary_column = unhex( @col3 )

    my $target_columns_string     = $self->COMPLEX_COLUMNS_FROM_TARGET( $modifier , $template_config , $parameters );
    my @target_column_expressions = $self->COMPLEX_COLUMNS_FROM_SOURCE( 'MIGRATION_IMPORT,WANT_ARRAY' , $template_config , $parameters );
    my @target_column_names       = $self->split_comma_separated_columns( $modifier , $target_columns_string, 1 );

    my @col_set_expressions;
    my $col_counter = 1;

    for my $target_column ( @target_column_names ) {
        my $this_import_expression = $target_column_expressions[ $col_counter - 1 ];
        my $this_col_expression = '@col' . $col_counter;
        $this_import_expression =~ s/$target_column/$this_col_expression/g;
        push @col_set_expressions
           , {
                 column_position => $this_col_expression
               , column_name     => $target_column
               , set_expression  => $this_import_expression
             };
        $col_counter ++;
    }

    my $set_clause = "( "
                   . join( " , " , map { $_->{column_position} } @col_set_expressions )
                   . " )\n"
                   . "set\n"
                   . join( " , " , map { $_->{column_name} . " = " . $_->{set_expression} } @col_set_expressions );

    return $set_clause;

}

sub name                        { return $_[0]->accessor( $IDX_RESOLVER_NAME,               $_[1] ); }

1;
