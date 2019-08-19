package SmartAssociates::Database::Item::Base;

use strict;
use warnings;

use base 'SmartAssociates::Base';

my $IDX_KEY_FIELD                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_DATABASE_TABLE                          =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_RECORD                                  =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_DBH                                     =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 3;
my $IDX_KEY_VALUE                               =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 4;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 5;

# This is an abstract class that serves as the base of other classes
# such as SmartAssociates::Database::Batch and SmartAssociates::Database::Job
# The basic idea is that we define a class per table that we want to
# access in a nice OO way. We define a list of managed fields. The logic
# to access ( query, update, delete ) the table exists here in the base
# class, and pulls in the fields and values from subclasses to construct
# the SQL

sub new {
    
    my $self = $_[0]->SUPER::new( $_[1] );
    
    return $self;
    
}

sub query {
    
    my $self = shift;
    
    my $key_value = $self->key_value;
    
    if ( ! defined $key_value ) {
        $self->log->fatal( "Can't call query() without setting a key field!" );
    }
    
    my $sql = "select *\n"
        . "from\n"
        . "    " . $self->database_table . "\n"
        . "where\n"
        . "    " . $self->key_field . " = ?";
    
    my $sth = $self->[ $IDX_DBH ]->prepare( $sql );
    
    $self->[ $IDX_DBH ]->execute( $sth, [ $key_value ] );
    
    $self->[ $IDX_RECORD ] = $sth->fetchrow_hashref;
    
    if ( ! defined $self->[ $IDX_RECORD ] ) {
        $self->log->fatal( "I was passed an invalid JOB_ID on the command line" );
    }
    
    $sth->finish;
    
}

sub fetchNextId {
    
    my $self = shift;
    
    $self->log->fatal( "Can't call fetchNextId() on SmartAssociates::Database::Item. Sub-classes MUST implement this method" );
    
}

sub insert {
    
    my $self = shift;
    
    $self->log->fatal( "Can't call insert() on SmartAssociates::Database::Item. Sub-classes MUST implement this method" );
    
}

sub update {
    
    my $self = shift;
    
    $self->log->fatal( "Can't call update() on SmartAssociates::Database::Item. Sub-classes MUST implement this method" );
    
}

sub field {
    
    my ( $self, $field, $value ) = @_;
    
    if ( defined $value ) {
        $self->[ $IDX_RECORD ]->{ $field } = $value;
    }
    
    return $self->[ $IDX_RECORD ]->{ $field };
    
}

sub key_field           { return $_[0]->accessor( $IDX_KEY_FIELD,          $_[1] ); }
sub database_table      { return $_[0]->accessor( $IDX_DATABASE_TABLE,     $_[1] ); }
sub dbh                 { return $_[0]->accessor( $IDX_DBH,                $_[1] ); }
sub key_value           { return $_[0]->accessor( $IDX_KEY_VALUE,          $_[1] ); }
sub record              { return $_[0]->accessor( $IDX_RECORD,             $_[1] ); }

1;
