package SmartAssociates::TemplateConfig::SQL::Iterator;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::SQL';

use constant VERSION                            => '1.0';

sub handle_executed_sth {
    
    my ( $self, $sth ) = @_;
    
    my $template_config = $self->template_record;
    
    my $iterator_store_name = $self->resolve_parameter( '#P_ITERATOR#' )
        || $self->log->fatal( "Iterators must define the [#P_ITERATOR#] parameter" );
    
    my $all_records = [];
    
    while ( my $record = $sth->fetchrow_hashref ) {
        # We want to rename all keys, replacing spaces with underscores.
        # We don't want to deal with spaces in tokens.
        foreach my $key ( keys %{$record} ) {
            if ( $key =~ /\s/ ) {
                my $this_key = $key;
                $this_key = s/\s/_/g;
                $record->{ $this_key } = $record->{ $key };
                delete $record->{ $key };
            }
        }
        push @{$all_records}, $record;
    }
    
    my $iterator = SmartAssociates::Iterator->new(
        $self->globals
      , $iterator_store_name
      , $all_records
    );
    
    $self->globals->ITERATOR( $iterator_store_name, $iterator );
    
    return $iterator->count_items;
    
}

1;
