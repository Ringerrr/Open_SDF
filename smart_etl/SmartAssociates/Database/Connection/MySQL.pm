package SmartAssociates::Database::Connection::MySQL;

use strict;
use warnings;

use base 'SmartAssociates::Database::Connection::Base';

use constant DB_TYPE            => 'MySQL';

sub default_port {
    
    my $self = shift;
    
    return 3306;
    
}

sub build_connection_string {
    
    my ( $self, $auth_hash ) = @_;
    
    my $connection_string =
          "dbi:mysql:"
        . "database="  . ( $auth_hash->{Database} || 'test' )
        . ";host="     . $auth_hash->{Host}
        . ";port="     . $auth_hash->{Port}
        . ";mysql_local_infile=1" # force-allow "load data infile local"
        . ";mysql_use_result=1";  # prevent $dbh->execute() from pulling all results into memory
    
    return $self->SUPER::build_connection_string( $auth_hash, $connection_string );
    
}

sub connect_pre {
    
    my ( $self, $auth_hash , $options_hash ) = @_;
    
    $options_hash->{dbi_options_hash} = {
        RaiseError        => 1
      , RaiseWarn         => 1
      , AutoCommit        => 1
      , mysql_enable_utf8 => 1
    };
    
    $auth_hash->{ConnectionString} = undef;
    
    return ( $auth_hash , $options_hash );
        
}

sub db_schema_table_string {
    
    my ( $self, $database, $schema, $table , $options ) = @_;
    
    if ( ! $options->{dont_quote} ) {
        return $database . '.' . $table;
    } else {
        return '"' . $database . '"."' . $table . '"';
    }
    
}

sub does_table_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select TABLE_NAME from information_schema.TABLES where TABLE_TYPE like 'BASE TABLE' and TABLE_SCHEMA = '" . $database . "' and TABLE_NAME = '" . $table . "'";

    return $sql;

}

sub does_database_not_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select 'not exists' where not exists ( select SCHEMA_NAME from `information_schema`.`SCHEMATA` where SCHEMA_NAME = '" . $database . "' )";

    return $sql;

}

sub create_database_string {

    my ( $self , $database ) = @_;

    return "create database `$database`";

}
sub does_schema_not_exist_string {

    my ( $self , $database , $schema , $table ) = @_;

    my $sql = "select 'not exists' ";

    return $sql;

}

sub quote_column_in_expression {

    my ( $self , $column , $expression ) = @_;

    $expression =~ s/$column/`$column`/g;
    return $expression;

}

sub create_expression_md5sum {

    my $self    = shift;
    my $options = shift;

    my $exp = $self->concatenate( $options );

    return "concat( '0x' , upper( md5( $exp ) ) ) as HASH_VALUE";

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
            $this_expression = "cast( $this_expression as CHAR )";
        }

        # TODO: optimise - don't coalesce NOT NULL columns
        push @all_expressions, $self->coalesce( $this_expression , "" );

    }

    my $return = "concat( " . join( ' , ' , @all_expressions ) . " )";

    return $return;

}

sub fetch_column_info {

    my ( $self, $db, $schema, $table ) = @_;

    $table =~ s/"//g; # Strip out quotes - we quote reserved words in get_fields_from_table()

    my $cached_field_metadata = $self->field_metadata_cache;

    if ( ! exists $cached_field_metadata->{ $schema }->{ $table } ) {

        use constant COLUMN_NAME    => 0;
        use constant DATA_TYPE      => 1;
        use constant NULLABLE       => 2;
        use constant COLUMN_NUMBER  => 3;
        use constant COLUMN_DEFAULT => 4;
        use constant PRECISION      => 5;

        my $sql = "select\n"
      . "    COLUMN_NAME\n"
      . "  , COLUMN_TYPE\n"
      . "  , IS_NULLABLE\n"
      . "  , ORDINAL_POSITION\n"
      . "  , COLUMN_DEFAULT\n"
      . "  , null as THE_PRECISION\n"
      . "from information_schema.columns\n"
      . "where table_schema = ? and table_name = ?\n"
      . "order by ordinal_position";

        my $sth = $self->prepare(
            $sql
        ) || return;

        $self->execute( $sth, [ $db, $table ] );

        my $column_info = $sth->fetchall_arrayref;

        my $field_metadata;

        # Split out the DATA_TYPE and PRECISION ...
        foreach my $column ( @{$column_info} ) {

            if ( $column->[ DATA_TYPE ] =~ /([\w\s]*)\((.*)\)/ ) {
                ( $column->[ DATA_TYPE ] , $column->[ PRECISION ] ) = ( $1 , $2 );
            }

            $field_metadata->{ $column->[ COLUMN_NAME ] } =
                {
                    COLUMN_NAME     => $column->[ COLUMN_NAME ]
                  , DATA_TYPE       => $column->[ DATA_TYPE ]
                  , PRECISION       => $column->[ PRECISION ]
                  , NULLABLE        => $column->[ NULLABLE ]
                  , COLUMN_DEFAULT  => $column->[ COLUMN_DEFAULT ]
                };

        }


        $cached_field_metadata->{ $schema }->{ $table } = $field_metadata;

        $self->field_metadata_cache( $cached_field_metadata );

    }

    return $cached_field_metadata->{ $schema }->{ $table };

}

1;
