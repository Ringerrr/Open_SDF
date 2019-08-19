package Database::ConfigManager::Postgres;

use warnings;
use strict;

use parent 'Database::ConfigManager';

use constant TYPE           => 'Postgres';

use Glib qw( TRUE FALSE );

sub create_simple_config {
    
    my $self = shift;
    
    # 'create table XXX if not exists' syntax was added in Postgres 9.x
    # So we can support earlier versions of Postgres, and also forks like Greenplum
    # we avoid that syntax and use the DB class' table_exists() method
    
    if (
        ! $self->{dbh}->table_exists(
            $self->{dbh}->{database}        # cheating, but whatever
          , "public"                        # we currently support *only* using the 'public' schema
          , "simple_config"
          )
    ) {
        
        $self->{dbh}->do(
            "create table simple_config (\n"
          . "    key    VARCHAR(200)            not null\n"
          . "  , value  VARCHAR(20000)\n"
          . "  , CONSTRAINT simple_config_primary_key PRIMARY KEY (key)"
          . ")"
        );
    }
    
}

1;
