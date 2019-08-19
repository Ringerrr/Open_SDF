package SmartAssociates::Database::Item::Job::Postgres;

use strict;
use warnings;

use base 'SmartAssociates::Database::Item::Job::Base';

use JSON;

my $IDX_JOB_ID_SEQ_STH                              =  SmartAssociates::Database::Item::Job::Base::FIRST_SUBCLASS_INDEX + 0;

use constant    FIRST_SUBCLASS_INDEX                => SmartAssociates::Database::Item::Job::Base::FIRST_SUBCLASS_INDEX + 1;

# This is the max size of executed SQL and associated messages *parts*
# ( we break up these items based on the max column size per DB subclass )

# While Postgres doesn't have a small max row / column length, it's still useful to restrict the message part size
# to some sane value, so we don't have driver issues getting the data in / out

use constant    MSG_PART_SIZE                       => 60000;

sub prepare {
    
    my $self = shift;
    
    $self->prepare_sequence_sql;
    
    $self->load_execution_sth(
        $self->dbh->prepare(
            "insert into load_execution (\n"
          . "    LOAD_EXECUTION_ID\n"
          . "  , JOB_ID\n"
          . "  , PROCESSING_GROUP_NAME\n"
          . "  , SEQUENCE_ORDER\n"
          . "  , TARGET_DB_NAME\n"
          . "  , TARGET_SCHEMA_NAME\n"
          . "  , TARGET_TABLE_NAME\n"
          . "  , START_TS\n"
          . "  , END_TS\n"
          . "  , PROCESSING_TIME\n"
          . "  , ROWS_AFFECTED\n"
          . "  , HOSTNAME\n"
          . "  , PERF_STATS\n"
          . "  , STEP_NOTES\n"
          . "  , TEMPLATE_NAME\n"
          . "  , WARNINGS\n"
          . ")\n"
          . "select\n"
          . "    ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , abs( extract( epoch from ?::timestamp ) - extract( epoch from ?::timestamp ) )\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?"
          . "  , ?"
        )
    );
    
    $self->log_parts_sth(
        $self->dbh->prepare(
            "insert into EXECUTION_LOG_PARTS (\n"
          . "    LOAD_EXECUTION_ID\n"
          . "  , LOG_TYPE\n"
          . "  , PART_SEQUENCE\n"
          . "  , PART_TEXT\n"
          . ") values (\n"
          . "    ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . "  , ?\n"
          . ")"
        )
    );
    
}

sub prepare_sequence_sql {
    
    my $self = shift;
    
    $self->load_execution_seq_sth(
        $self->dbh->prepare(
            "select nextval('LOAD_EXECUTION_SEQ')"
        )
    );
    
    $self->[ $IDX_JOB_ID_SEQ_STH ] = $self->dbh->prepare(
        "select nextval('JOB_CTL_SEQ')"
    );
    
}

sub log_execution {
    
    my ( $self , $processing_group_name , $sequence_order , $tgt_db_nme , $tgt_schema_nme
       , $tgt_tbl_nme , $start_ts , $end_ts , $record_count , $error , $template_sql
       , $perf_stats , $notes , $custom_logs ) = @_;
    
    $self->dbh->execute(
        $self->load_execution_seq_sth
    );
    
    my $rec = $self->load_execution_seq_sth->fetchrow_arrayref;
    my $load_execution_id = $rec->[0];
    
    # Postgres can't handle timestamps without delimiters. Enterprise database.
    foreach my $item ( $start_ts , $end_ts ) {
        $item =~ /([\d]{4})([\d]{2})([\d]{2})_([\d]{2})([\d]{2})([\d]{2})/;
        $item = $1 . "-" . $2 . "-" . $3 . " " . $4 . ":" . $5 . ":" . $6;
    }

    my $current_template = $self->globals->CURRENT_TEMPLATE_CONFIG();

    $self->dbh->execute(
        $self->load_execution_sth
      , [
            $load_execution_id
          , $self->key_value
          , $processing_group_name
          , $sequence_order
          , $tgt_db_nme
          , $tgt_schema_nme
          , $tgt_tbl_nme
          , $start_ts
          , $end_ts
          , $end_ts
          , $start_ts
          , ( $record_count eq &SmartAssociates::Database::Connection::Base::PERL_ZERO_RECORDS_INSERTED ? 0 : $record_count )
          , $self->hostname()
          , $perf_stats
          , $notes
          , $current_template->template_record->{TEMPLATE_NAME}
          , to_json( $self->log->warnings() , { pretty => 1 } )
        ]
    );

    $self->log->clear_warnings();

    $self->part_to_log(
        $load_execution_id
      , SmartAssociates::Database::Item::Job::Base::MSG_PART_TYPE_LOG
      , $template_sql
    );
    
    if ( $error ) {
        
        $self->part_to_log(
            $load_execution_id
          , SmartAssociates::Database::Item::Job::Base::MSG_PART_TYPE_ERROR
          , $error
        );
        
        $self->field(
            SmartAssociates::Database::Item::Job::Base::MSG_PART_TYPE_ERROR
          , substr( $error, 0, 20000 )
        );
        
    }

    if ( $custom_logs ) {

        foreach my $log_type ( keys %{$custom_logs} ) {
            $self->part_to_log(
                $load_execution_id
              , $log_type
              , $custom_logs->{ $log_type }
            );
        }

    }

}

sub fetchNextId {
    
    my $self = shift;
    
    $self->dbh->execute(
        $self->[ $IDX_JOB_ID_SEQ_STH ]
    );
    
    my $rec = $self->[ $IDX_JOB_ID_SEQ_STH ]->fetchrow_arrayref;
    my $job_id = $rec->[0];
    
    return $job_id;
    
}

sub update {
    
    my $self = shift;
    
    my $sql = "update JOB_CTL set\n"
            . "    END_TS          = now()::timestamp\n"
            . "  , STATUS          = ?\n"
            . "  , PROCESSING_TIME = extract( epoch from now()::timestamp ) - extract( epoch from start_ts::timestamp )\n"
            . "  , JOB_ARGS        = ?\n"
            . "  , ERROR_MESSAGE   = ?\n"
            . "  , LOG_PATH        = ?\n"
            . "where\n"
            . "    JOB_ID          = ?\n";
    
    my $sth = $self->dbh->prepare( $sql );
    
    my $rec = $self->record;
    
    $self->dbh->execute(
        $sth
      , [
            $rec->{ &SmartAssociates::Database::Item::Job::Base::FLD_STATUS }
          , $rec->{ &SmartAssociates::Database::Item::Job::Base::FLD_JOB_ARGS }
          , $rec->{ &SmartAssociates::Database::Item::Job::Base::FLD_ERROR_MESSAGE }
          , $rec->{ &SmartAssociates::Database::Item::Job::Base::FLD_LOG_PATH }
          , $self->key_value
        ]
    );
    
    $sth->finish();
    
}

1;
