package SmartAssociates::Iterator;

use strict;
use warnings;

use Carp;

use base 'SmartAssociates::Base';

my $IDX_ITERATOR_NAME                           =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_POSITION                                =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_DATA_ARRAY                              =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 2;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 3;

sub new {
    
    my $self = $_[0]->SUPER::new(    $_[1] );
    
    $self->[ $IDX_ITERATOR_NAME ]  = $_[2];
    $self->[ $IDX_DATA_ARRAY ]     = $_[3] || [];
    
    $self->[ $IDX_POSITION ]       = 0;
    
    return $self;
    
}

sub iterate {
    
    my $self = shift;
    
    if ( $self->[ $IDX_POSITION ] == scalar @{ $self->[ $IDX_DATA_ARRAY ] } - 1 ) { # position starts at 0
        $self->[ $IDX_POSITION ] = 0;
    } else {
        $self->[ $IDX_POSITION ] ++;
    }
    
    return $self->[ $IDX_POSITION ];
    
}

sub current_record {
    
    my $self = shift;
    
    return $self->[ $IDX_DATA_ARRAY ]->[ $self->[ $IDX_POSITION ] ];
    
}

sub get_item {
    
    my ( $self, $item_name ) = @_;
    
    my $rec = $self->current_record;
    
    if ( exists $rec->{ $item_name } ) {
        return $rec->{ $item_name };
    } else {
        $self->log->warn( "Iterator [" . $self->[ $IDX_ITERATOR_NAME ] . "] was asked for data item [$item_name], but it doesn't exist in this recordset!" );
        return undef;
    }
    
}

sub count_items {
    
    my $self = shift;
    
    return scalar @{ $self->[ $IDX_DATA_ARRAY ] };
    
}

sub push {

    my ( $self, $record ) = @_;

    push @{ $self->[ $IDX_DATA_ARRAY ] }, $record;

}

1;
