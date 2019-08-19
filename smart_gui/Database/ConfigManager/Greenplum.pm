package Database::ConfigManager::Greenplum;

use strict;

use parent 'Database::ConfigManager::Postgres';

use constant TYPE           => 'Greenplum';

use Glib qw( TRUE FALSE );

1;