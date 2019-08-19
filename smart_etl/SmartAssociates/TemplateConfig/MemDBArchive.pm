package SmartAssociates::TemplateConfig::MemDBArchive;

use strict;
use warnings;

use JSON;
use Data::Dumper;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    my $mem_dbh = $self->processing_group->target_database(
        &SmartAssociates::Database::Connection::Memory::DB_TYPE
      , &SmartAssociates::Database::Connection::Memory::DB_TYPE
      , &SmartAssociates::Database::Connection::Memory::DB_TYPE
    );
    
    eval {
        $mem_dbh->dbh->sqlite_backup_to_file(
            $self->log->log_dir
          . $self->globals->DIR_SEPARATOR . $self->resolve_parameter( '#ENV_JOB_ID#' ) . '.db'
        ) || die( $mem_dbh->dbh->errstr );
    };
    
    my $error = $@;
    
    my $end_ts = $self->log->prettyTimestamp();
    
    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $self->detokenize( $template_config->{TARGET_DB_NAME} )
      , $self->detokenize( $template_config->{TARGET_SCHEMA_NAME} )
      , $self->detokenize( $template_config->{TARGET_TABLE_NAME} )
      , $start_ts
      , $end_ts
      , ( $error ? 0 : -1 )
      , $error
      , $template_config->{TEMPLATE_SQL}
      , undef
      , $template_config->{NOTES}
    );
    
}

1;
