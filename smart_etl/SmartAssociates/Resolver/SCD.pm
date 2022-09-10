package SmartAssociates::Resolver::SCD;

use strict;
use warnings;

use Carp;

use base 'SmartAssociates::Resolver::Base';

# my $IDX_RESOLVER_NAME                           =  SmartAssociates::Resolver::Base::FIRST_SUBCLASS_INDEX + 0;

# use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Resolver::Base::FIRST_SUBCLASS_INDEX + 1;

sub COMPLEX_JOIN_ON_KEYS {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $keys = $template_config->resolve_parameter( '#P_KEYS#' );

    if ( ! $keys ) {
        $self->log->fatal( "This template requires the parameter #P_KEYS# to be set" );
    }

    my $field_metadata = $template_config->target_database->fetch_column_info(
        $template_config->detokenize( $template_config->template_record->{TARGET_DB_NAME} )
      , $template_config->detokenize( $template_config->template_record->{TARGET_SCHEMA_NAME} )
      , $template_config->detokenize( $template_config->template_record->{TARGET_TABLE_NAME} )
    );

    my @join_conditions;

    my $case_insensitive_versioning = $template_config->resolve_parameter( '#P_ZZ_CASE_INSENSITIVE_VERSIONING#' );

    foreach my $key ( split ',', $keys ) {

        $key =~ s/\s//g; # strip spaces

        if ( $case_insensitive_versioning
            && $field_metadata->{ $key }->{DATA_TYPE} =~ /CHAR|STRING/
        ) {
            push @join_conditions, "lower(SOURCE.$key) = lower(TARGET.$key)";
        } else {
            push @join_conditions, "SOURCE.$key = TARGET.$key";
        }

    }

    my $value = join( " and ", @join_conditions );

    return $value;

}

sub COMPLEX_SOURCE_KEYS {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    if ( ! $parameters->{'#P_KEYS#'} ) {
        $self->log->fatal( "This template requires the parameter #P_KEYS# to be set" );
    }

    my @keys;

    my $keys_string = $template_config->resolve_parameter( '#P_KEYS#' );

    foreach my $key (split ',', $keys_string) {
        $key =~ s/\s//g;
        push @keys, "SOURCE.$key";
    }

    my $value = join( " , ", @keys );

    return $value;

}

sub COMPLEX_SCD2_ATTRIBUTE_CHANGED {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $field_metadata = $template_config->target_database->fetch_column_info(
        $template_config->detokenize( $template_config->template_record->{TARGET_DB_NAME} )
      , $template_config->detokenize( $template_config->template_record->{TARGET_SCHEMA_NAME} )
      , $template_config->detokenize( $template_config->template_record->{TARGET_TABLE_NAME} )
    );

    my @ignore_cols;

    {
        no warnings 'uninitialized';
        @ignore_cols = split( ',', $template_config->resolve_parameter( '#P_ZZ_IGNORE_COLUMNS#' ) );
    }

    foreach my $item (@ignore_cols) {
        $item =~ s/^\s+|\s+$//g;
    }

    my @comparisons;

    foreach my $field ( keys %{$field_metadata} ) {

        if ( grep { $_ eq $field } @ignore_cols ) {
            next; # next item in foreach
        }

        if ( $template_config->resolve_parameter( '#P_ZZ_CASE_INSENSITIVE_VERSIONING#' )
            && $field_metadata->{ $field }->{DATA_TYPE} =~ /CHAR|STRING/
        ) {
            push @comparisons
                , '      lower(SOURCE.'.$field.') <> lower(TARGET.'.$field.')';
        } else {
            push @comparisons
                , '      SOURCE.'.$field.' <> TARGET.'.$field;
        }

        if ( $field_metadata->{ $field }->{NULLABLE} ) {

            push @comparisons
                , '    ( SOURCE.'.$field.' is null and TARGET.'.$field.' is not null )';

            push @comparisons
                , '    ( SOURCE.'.$field.' is not null and TARGET.'.$field.' is null )';

        }

    }

    if ( ! @comparisons ) {
        push @comparisons, "'Dan rocks' != 'Dan rocks'";
    }

    my $comparison_sql_component = join( "\n  or ", @comparisons );

    return $comparison_sql_component;

}

sub COMPLEX_TARGET_KEY_IS_NULL {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    if ( ! $parameters->{'#P_KEYS#'}->{PARAM_VALUE} ) {
        $self->log->fatal( "This template requires the parameter #P_KEYS# to be set" );
    }

    my @join_conditions;

    foreach my $key ( split ',', $parameters->{'#P_KEYS#'}->{PARAM_VALUE} ) {
        $key =~ s/\s//g;
        push @join_conditions, "TARGET.$key is null";
    }

    my $value = join( " and ", @join_conditions );

    return $value;

}

sub COMPLEX_SOURCE_KEY_IS_NULL {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    if ( ! $parameters->{'#P_KEYS#'}->{PARAM_VALUE} ) {
        $self->log->fatal( "This template requires the parameter #P_KEYS# to be set" );
    }

    my @join_conditions;

    foreach my $key (split ',', $parameters->{'#P_KEYS#'}->{PARAM_VALUE}) {
        $key =~ s/\s//g;
        push @join_conditions, "SOURCE.$key is null";
    }

    my $value = join( " and ", @join_conditions );

    return $value;

}

sub COMPLEX_BIGQUERY_SCD1_MERGE_COLUMNS_FROM_SOURCE_IF_EXISTS {

    my ( $self, $modifier , $template_config, $parameters ) = @_;

    my $keys_string = $template_config->resolve_parameter( '#P_KEYS#' );
    my @keys = $self->split_comma_separated_columns( $keys_string, 1 );
    my @comparisons;

    foreach my $key (@keys) {
        push @comparisons, "SOURCE.$key is null";
    }

    my $source_record_is_null = "( " . join( " and ", @comparisons ) . " )";

    my $target_db     = $template_config->template_record->{TARGET_DB_NAME};
    my $target_schema = $template_config->template_record->{TARGET_SCHEMA_NAME};
    my $target_table  = $template_config->template_record->{TARGET_TABLE_NAME};

    $target_db = $self->detokenize( $target_db );
    $target_schema = $self->detokenize( $target_schema );
    $target_table = $self->detokenize( $target_table );

    my $fields;

    # For BigQuery, we *don't* want to sort the fields, as this will change the order of fields in the table ( we re-materialise )
    $fields = $template_config->target_database->get_fields_from_table(
        $target_db
      , $target_schema
      , $target_table
      , { dont_sort => 1 }
    );

    my $ignore_columns = $self->detokenize( '#P_ZZ_IGNORE_COLUMNS#' );
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

        push @final_columns, "case when $source_record_is_null then TARGET.$field else SOURCE.$field end as $field";

    }

    my $value = join(
        "\n  , "
        , @final_columns
    );

    return $value;

}

1;
