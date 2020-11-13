package SmartAssociates::TemplateConfig::SQL::Q_Params;

use strict;
use warnings;

use base 'SmartAssociates::TemplateConfig::SQL';

use constant VERSION                            => '1.0';

sub handle_executed_sth {
    
    my ( $self, $sth ) = @_;
    
    my $template_config = $self->template_record;
    
    my $result_row = $sth->fetchrow_hashref();
    
    my $query_parameters = $self->globals->Q_PARAMS;
    
    foreach my $column ( keys %{$result_row} ) {
        # We want to rename all keys, replacing spaces with underscores.
        # We don't want to deal with spaces in tokens.
        my $this_column = $column;
        $this_column =~ s/\s/_/;
        $query_parameters->{ $this_column } = $result_row->{ $column };
        $self->log->info( "Setting Q Param [$this_column] to value [" . $result_row->{ $column} . "]" );
    }
    
    $self->globals->Q_PARAMS( $query_parameters );
    
    return 1;
    
}

1;
