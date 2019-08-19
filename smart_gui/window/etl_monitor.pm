package window::etl_monitor;

use parent 'window';

use strict;
use warnings;

use Glib qw( TRUE FALSE );

use Data::Dumper;

use feature 'switch';

sub new {
    
    my ( $class, $globals, $options, $batch_id ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{mem_dbh} = Database::Connection::SQLite->new(
        $self->{globals}
      , {
            location    => ":memory:"
        }
    );
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "ETL_Monitor"
    );
    
    $self->{builder}->connect_signals( undef, $self );
    
    $self->get_window->maximize;
    
    $self->{progress} = $self->{builder}->get_object( "progress" );
    
    # Syntax highlighting
    my $view_buffer = Gtk3::SourceView::Buffer->new_with_language( $self->{globals}->{gtksourceview_language} );
    $view_buffer->set_highlight_syntax( TRUE );
    
    $view_buffer->set_style_scheme( $self->{globals}->{gtksourceview_scheme} );
    
    my $source_view = $self->{builder}->get_object( 'EXECUTED_TEMPLATE' );
    
    if ( Gtk3::MINOR_VERSION >= 16 ) {
        $source_view->set_background_pattern( 'GTK_SOURCE_BACKGROUND_PATTERN_TYPE_GRID' );
    }
    
    $source_view->set_buffer( $view_buffer );
    
    if ( $batch_id ) {
        $self->{builder}->get_object( "BatchID" )->set_text( $batch_id );
        $self->{builder}->get_object( "BatchID_Filter" )->set_active( 1 );
    }
    
    $self->{jobs} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{globals}->{config_manager}->sdf_connection( "LOG" )
      , read_only               => 1
      , on_row_select           => sub { $self->on_jobs_row_select( @_ ) }
      , primary_keys            => [ "JOB_ID" ]
      , auto_incrementing       => 0
      , column_sorting          => 1
      , force_upper_case_fields => 1
      , sql                     => {
                                        select    => "BATCH_ID, IDENTIFIER, JOB_ID, PROCESSING_GROUP_NAME, STATUS, PROCESSING_TIME, HOSTNAME"
                                      , from      => "JOB_CTL"
                                      , where     => "BATCH_ID = ?"
                                      , order_by  => "JOB_ID"
                                      , bind_values => [ 0 ]
                                   }
      , fields                  => [
                                        {
                                            name            => "BATCH_ID"
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "IDENTIFIER"
                                          , x_percent       => 35
                                        }
                                      , {
                                            name            => "JOB_ID"
                                          , x_percent       => 10
                                          , number          => { separate_thousands  => TRUE } # this also activates numeric sorting
                                        }
                                      , {
                                            name            => "PROCESSING_GROUP_NAME"
                                          , x_percent       => 25
                                        }
                                      , {
                                            name            => "STATUS"
                                          , x_percent       => 10
                                        }
                                      , {
                                            name            => "PROCESSING_TIME"
                                          , x_percent       => 20
                                          , renderer        => "number"
                                          , number          => { separate_thousands  => TRUE } # this also activates numeric sorting
                                        }
                                      , {
                                            name            => "HOST_NAME"
#                                          , renderer        => "hidden"
                                        }
                                   ]
      , treeview                => $self->{builder}->get_object( "Jobs" )
    } );
    
    $self->{steps} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{globals}->{config_manager}->sdf_connection( "LOG" )
      , read_only               => 1
      , on_row_select           => sub { $self->on_steps_row_select( @_ ) }
      , primary_keys            => [ "LOAD_EXECUTION_ID" ]
      , auto_incrementing       => 0
      , column_sorting          => 1
      , force_upper_case_fields => 1
      , sql                     => {
                                        select    => "LOAD_EXECUTION_ID , TEMPLATE_NAME , TARGET_DB_NAME , TARGET_TABLE_NAME , PROCESSING_TIME , ROWS_AFFECTED , PERF_STATS , WARNINGS"
                                      , from      => "LOAD_EXECUTION"
                                      , order_by  => "START_TS"
                                      , where     => "JOB_ID = ?"
                                      , bind_values => [ 0 ]
                                   }
      , fields                  => [
                                        {
                                            name            => "LOAD_EXECUTION_ID"
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "Template"
                                          , x_percent       => 40
                                        }
                                      , {
                                            name            => "DB"
                                          , x_percent       => 30
                                        }
                                      , {
                                            name            => "Table"
                                          , x_percent       => 30
                                        }
                                      , {
                                            name            => "Seconds"
                                          , x_absolute      => 80
                                          , renderer        => "number"
                                          , number          => { separate_thousands  => TRUE } # this also activates numeric sorting
                                        }
                                      , {
                                            name            => "Rows"
                                          , x_absolute      => 80
                                          , renderer        => "number"
                                          , number          => { separate_thousands  => TRUE } # this also activates numeric sorting
                                        }
                                      , {
                                            name            => "PERF_STATS"
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "WARNINGS"
                                          , renderer        => "hidden"
                                        }
                                    ]
      , treeview                => $self->{builder}->get_object( "Steps" )
    } );
    
    $self->{active_batches} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{globals}->{config_manager}->sdf_connection( "LOG" )
      , read_only               => 1
      , on_row_select           => sub { $self->on_batch_row_select( 'active_batches' ) }
      , primary_keys            => [ "BATCH_ID" ]
      , auto_incrementing       => 0
      , column_sorting          => 1
      , force_upper_case_fields => 1
      , sql                     => {
                                        select    => "*"
                                      , from      => "BATCH_CTL"
                                      , where     => "STATUS not like 'COMPLETE%' and STATUS != 'READY' and STATUS != 'KILLED'"
                                      , order_by  => "BATCH_ID desc"
                                   }
      , fields                  => [
                                        {
                                            name            => "Batch"
                                          , x_absolute      => 70
                                          , number          => { separate_thousands  => TRUE } # this also activates numeric sorting
                                        }
                                      , {
                                            name            => "Identifier"
                                          , x_percent       => 60
                                        }
                                      , {
                                            name            => "Start"
                                          , x_percent       => 25
                                        }
                                      , {
                                            name            => "End"
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "Status"
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "Seconds"
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "Hostname"
                                          , x_percent       => 15
                                        }
        ]
      , treeview                => $self->{builder}->get_object( "ActiveBatches" )
    } );
    
    $self->get_completed_batches();
    
    $self->{complete_batches} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{mem_dbh}
      , read_only               => 1
      , on_row_select           => sub { $self->on_batch_row_select( 'complete_batches' ) }
      , primary_keys            => [ "BATCH_ID" ]
      , auto_incrementing       => 0
      , column_sorting          => 1
      , force_upper_case_fields => 1
      , sql                     => {
                                            select          => "*"
                                          , from            => "COMPLETE_BATCHES"
                                          , order_by        => "BATCH_ID desc" 
                                   }
      , fields                  => [
                                        {
                                            name            => "Batch"
                                          , x_absolute      => 65
                                          , number          => { separate_thousands  => TRUE } # this also activates numeric sorting
                                        }
                                      , {
                                            name            => "Identifier"
                                          , x_percent       => 60
                                        }
                                      , {
                                            name            => "Start"
#                                          , x_percent       => 20
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "End"
#                                          , x_percent       => 20
                                          , renderer        => "hidden"
                                        }
                                      , {
                                            name            => "Status"
                                          , x_percent       => 40
                                        }
                                      , {
                                            name            => "Seconds"
                                          , x_absolute      => 70
                                          , renderer        => "number"
                                          , number          => { separate_thousands  => TRUE } # this also activates numeric sorting
                                        }
                                      , {
                                            name            => "Hostname"
                                          , renderer        => "hidden"
                                      }
        ]
      , vbox                    => $self->{builder}->get_object( "CompleteBatches_box" )
      , recordset_extra_tools   => {
                                        delete_batch        => {
                                            type            => 'button'
                                          , markup          => "<b><span color='red'>delete batch</span></b>"
                                          , icon_name       => 'edit-delete'
                                          , coderef         => sub { $self->delete_batch }
                                        }
        }
      , recordset_tool_items    => [ qw | delete_batch data_to_csv | ]
      , auto_tools_box          => 1
      #, treeview                => $self->{builder}->get_object( "CompleteBatches" )
    } );
    
    $self->{datacop_totals} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{globals}->{config_manager}->sdf_connection( "LOG" )
      , read_only               => 1
      #, primary_keys            => [ "BATCH_ID" ]
      , auto_incrementing       => 0
      , column_sorting          => 1
      , force_upper_case_fields => 1
      , sql                     => {
                                            select          => "RECORDS_VALIDATED, RECORDS_REJECTED"
                                          , from            => "DATA_COP_TOTALS"
                                          , where           => "job_id = ?"
                                          , bind_values     => [ 0 ]
                                   }
      , vbox                    => $self->{builder}->get_object( 'DataCop_Totals' )
    } );
    
    $self->{datacop_issues_summary} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{globals}->{config_manager}->sdf_connection( "LOG" )
      , read_only               => 1
      #, primary_keys            => [ "BATCH_ID" ]
      , auto_incrementing       => 0
      , column_sorting          => 1
      , force_upper_case_fields => 1
      , sql                     => {
                                            select          => "COLUMN_NAME, COLUMN_TYPE, COLUMN_TYPE_EXTRA, ISSUE_TYPE, ISSUE_COUNT"
                                          , from            => "DATA_COP_ISSUES_SUMMARY"
                                          , where           => "job_id = ?"
                                          , bind_values     => [ 0 ]
                                   }
      , vbox                    => $self->{builder}->get_object( 'DataCop_IssuesSummary' )
    } );
    
    if ( $self->manage_widget_value( 'AutorefreshTimeout' ) ) {
        $self->on_StartAutoRefresh_clicked;
    }
    
    return $self;
    
}

sub get_completed_batches {
    
    my $self = shift;
    
    my $filter;
    
    if ( $self->{builder}->get_object( "BatchID_Filter" )->get_active ) {
        
        $filter = "where B.BATCH_ID in ( " . $self->{builder}->get_object( "BatchID" )->get_text . " )";
        
    } elsif ( $self->{builder}->get_object( "Today" )->get_active ) {
        
        $filter = "where  B.STATUS in ( 'COMPLETE', 'COMPLETE_WITH_ERROR' )\n"
                . "and now() - interval '1 days' <= B.START_TS";
        
    } elsif ( $self->{builder}->get_object( "ThisWeek" )->get_active ) {
        
        $filter = "where  B.STATUS in ( 'COMPLETE', 'COMPLETE_WITH_ERROR' )\n"
                . "and now() - interval '7 days' <= B.START_TS";
        
    } else {
        
        $filter = "where  B.STATUS in ( 'COMPLETE', 'COMPLETE_WITH_ERROR' )\n"
        
    }
    
    my $sql = "select B.BATCH_ID, B.BATCH_IDENTIFIER, B.START_TS, B.END_TS, B.STATUS, B.PROCESSING_TIME, B.HOSTNAME\n"
            . "from   BATCH_CTL B left join JOB_CTL J on B.BATCH_ID = J.BATCH_ID\n"
            . "$filter\n"
            . "group by B.BATCH_ID, B.BATCH_IDENTIFIER, B.START_TS, B.END_TS, B.STATUS, B.PROCESSING_TIME, B.HOSTNAME";
    
    print "\n\n$sql\n\n";
    
    if ( $self->{batch_filter_sql} && $self->{batch_filter_sql} eq $sql ) {
        # Don't requery with exactly the same args
        return;
    }
    
    $self->{batch_filter_sql} = $sql;
    
    my $dbh = $self->{globals}->{config_manager}->sdf_connection( "LOG" );
    
    my $sth = $dbh->prepare( $sql )
        || return;
    
    $dbh->execute( $sth )
        || return;
    
    $dbh->sth_2_sqlite(
        $sth
      , [
            {
                name    => "BATCH_ID"
              , type    => "number"
            }
          , {
                name    => "BATCH_IDENTIFIER"
              , type    => "number"
            }
#          , {
#                name    => "PROCESS_GRP_NME"
#              , type    => "text"
#            }
          , {
                name    => "START_TS"
              , type    => "text"
            }
          , {
                name    => "END_TS"
              , type    => "text"
            }
          , {
                name    => "STATUS"
              , type    => "text"
            }
          , {
                name    => "PROCESSING_TIME"
              , type    => "number"
            }
          , {
                name    => "HOSTNAME"
              , type    => "text"
            }
        ]
      , $self->{mem_dbh}
      , "COMPLETE_BATCHES"
    );
    
    if ( exists $self->{complete_batches} ) {
        $self->{complete_batches}->query();
        # Select the 1st row in the completed batches datasheet
        my $model = $self->{complete_batches}->{treeview}->get_model;
        my $iter = $model->get_iter_first;

        if ( $iter ) {
            my $treeselection = $self->{complete_batches}->{treeview}->get_selection;
            $treeselection->select_iter( $iter );
        }
    }

}

sub delete_batch {
    
    my $self = shift;
    
    my $batch_id = $self->{complete_batches}->get_column_value( "BATCH_ID" );
    
    if ( ! $batch_id ) {
        return;
    }
    
    my $dbh = $self->{globals}->{config_manager}->sdf_connection( "LOG" );
    
    $dbh->do( "delete from execution_log_parts where load_execution_id in\n"
            . " ( select load_execution_id from load_execution where job_id in\n"
            . "     ( select job_id from job_ctl where batch_id = ? )\n"
            . " )"
            , [ $batch_id ]
    );
    
    $dbh->do( "delete from load_execution where job_id in ( select job_id from job_ctl where batch_id = ? )", [ $batch_id ] );
    
    $dbh->do( "delete from job_ctl where batch_id = ?", [ $batch_id ] );
    
    $dbh->do( "delete from batch_ctl where batch_id = ?", [ $batch_id ] );
    
    $self->get_completed_batches();
    
}

sub on_ActiveBatches_refresh_clicked {
    
    my $self = shift;
    
    $self->{active_batches}->query();
    
}

sub on_batch_row_select{
    
    my ( $self, $type ) = @_;
    
    my $batch_id = $self->{$type}->get_column_value( "BATCH_ID" );
    
    $self->{jobs}->query( { bind_values => [ $batch_id ] } );
    
    $self->{steps}->query( { bind_values => [ 0 ] } );
    
    $self->{builder}->get_object( "EXECUTED_TEMPLATE" )->get_buffer->set_text( '' );
    $self->{builder}->get_object( "ERROR_MSG" )->get_buffer->set_text( '' );
    
    # Select 1st job in batch
    my $model = $self->{jobs}->{treeview}->get_model;
    my $iter = $model->get_iter_first;
    
    if ( $iter ) {

        $self->{jobs}->{treeview}->get_selection->select_iter( $iter );

        # Also select the *last* step in the job ( the main reason we'd be in here is looking for errors, and
        # errors will typically be in the last step ...
        $model = $self->{steps}->{treeview}->get_model;
        $iter = $model->get_iter_first;
        my $last_string;

        while ( $iter ) {
            $last_string = $model->get_string_from_iter( $iter );
            if ( ! $model->iter_next( $iter ) ) {
                last;
            }
        }

        if ( defined $last_string ) {
            $self->{steps}->{treeview}->get_selection->select_iter( $model->get_iter_from_string( $last_string ) );
        }

    } else {

        $self->{steps}->query( { bind_values => [ 0 ] } );
        $self->{datacop_totals}->query( { bind_values => [ 0 ] } );
        $self->{datacop_issues_summary}->query( { bind_values => [ 0 ] } );

    }
    
}

sub on_jobs_row_select {
    
    my $self = shift;
    
    my $job_id = $self->{jobs}->get_column_value( "JOB_ID" );
    
    $self->{steps}->query( { bind_values => [ $job_id ] } );
    $self->{datacop_totals}->query( { bind_values => [ $job_id ] } );
    $self->{datacop_issues_summary}->query( { bind_values => [ $job_id ] } );
    
    $self->{builder}->get_object( "EXECUTED_TEMPLATE" )->get_buffer->set_text( '' );
    $self->{builder}->get_object( "ERROR_MSG" )->get_buffer->set_text( '' );
    
}

sub on_steps_row_select {
    
    my $self = shift;
    
    my $le_id = $self->{steps}->get_column_value( "LOAD_EXECUTION_ID" );
    
    my $log_db = $self->{globals}->{config_manager}->sdf_connection( "LOG" );
    
    my $log_stuff;

    # The standard log types - log and error ...
    foreach my $type ( "log", "error" ) {
        
        my $sth = $log_db->prepare(
            "select PART_TEXT from EXECUTION_LOG_PARTS\n"
          . "where LOAD_EXECUTION_ID = ? and LOG_TYPE = ?\n"
          . "order by PART_SEQUENCE"
        ) || die( $log_db->errstr );
        
        $sth->execute( $le_id, $type )
            || die( $sth->errstr );
        
        while ( my $row = $sth->fetchrow_arrayref ) {
            $log_stuff->{ $type } .= $$row[0];
        }
        
        $sth->finish();
        
    }
    
    $self->{builder}->get_object( "PERF_STATS" )->get_buffer->set_text( $self->{steps}->get_column_value( "PERF_STATS" ) || '' );
    $self->{builder}->get_object( "EXECUTED_TEMPLATE" )->get_buffer->set_text( $log_stuff->{log} || '' );

    # Warnings
    my $warnings = $self->{steps}->get_column_value( "WARNINGS" );
    my $page_focus_grab;

    if ( $warnings && $warnings ne "[]\n" ) {
        $self->{builder}->get_object( "WARNINGS_MSG" )->get_buffer->set_text( $warnings );
        $self->{builder}->get_object( "Warnings_label" )->set_markup( "<span color='red'><b>Warnings</b></span>" );
        my $focused_page = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();;
        if ( $focused_page != 3 && $focused_page !=4 ) {
            $self->{previous_active_page} = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();
        }
        $page_focus_grab = 1;
        $self->{builder}->get_object( 'ExecutionLog_notebook' )->set_current_page( 3 );
    } else {
        $self->{builder}->get_object( "WARNINGS_MSG" )->get_buffer->set_text( '' );
        $self->{builder}->get_object( "Warnings_label" )->set_markup( "<b>Warnings</b>" );
    }

    if ( $log_stuff->{error} ) {
        $self->{builder}->get_object( "ERROR_MSG" )->get_buffer->set_text( $log_stuff->{error} );
        $self->{builder}->get_object( "Errors_label" )->set_markup( "<span color='red'><b>Errors</b></span>" );
        if ( ! $page_focus_grab ) {
            my $focused_page = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();;
            if ( $focused_page != 3 && $focused_page !=4 ) {
                $self->{previous_active_page} = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();
            }
        }
        $self->{builder}->get_object( 'ExecutionLog_notebook' )->set_current_page( 4 );
    } else {
        $self->{builder}->get_object( "ERROR_MSG" )->get_buffer->set_text( '' );
        $self->{builder}->get_object( "Errors_label" )->set_markup( "<b>Errors</b>" );
    }

    if ( ! $page_focus_grab ) {
        $self->{builder}->get_object( 'ExecutionLog_notebook' )->set_current_page( ( $self->{previous_active_page} || 0 ) );
    }

    # Custom logs
    my $custom_logs_box = $self->{builder}->get_object( 'CustomLogs_box' );

    foreach my $item ( $custom_logs_box->get_children ) {
        $item->destroy();
    }

    my $sth = $log_db->prepare(
        "select LOG_TYPE from EXECUTION_LOG_PARTS\n"
      . "where LOAD_EXECUTION_ID = ? and LOG_TYPE not in ( 'log' , 'error' )\n"
      . "group by LOG_TYPE"
    ) || die( $log_db->errstr );

    $sth->execute( $le_id )
        || die( $sth->errstr );

    my $notebook;

    while ( my $row = $sth->fetchrow_hashref ) {

        my $custom_log_sth = $log_db->prepare(
            "select PART_TEXT from EXECUTION_LOG_PARTS\n"
           . "where LOAD_EXECUTION_ID = ? and LOG_TYPE = ?\n"
           . "order by PART_SEQUENCE"
        ) || die( $log_db->errstr );

        $custom_log_sth->execute( $le_id, $row->{LOG_TYPE} )
            || die( $sth->errstr );

        my $custom_log_text;

        while ( my $custom_log_row = $custom_log_sth->fetchrow_hashref ) {
            $custom_log_text .= $custom_log_row->{PART_TEXT};
        }

        my $this_buffer = Gtk3::TextBuffer->new();
        $this_buffer->set_text( $custom_log_text || '' );
        my $this_textview = Gtk3::TextView->new_with_buffer( $this_buffer );
        my $this_scrolled_window = Gtk3::ScrolledWindow->new();
        $this_scrolled_window->add( $this_textview );

        if ( ! $notebook ) {
            $notebook = Gtk3::Notebook->new();
            $notebook->set( 'tab-pos' , 'left' );
        }

        my $label = Gtk3::Label->new();
        $label->set_markup( "<b>" . $row->{LOG_TYPE} . "</b>" );

        $notebook->append_page( $this_scrolled_window , $label );

    }

    if ( $notebook ) {
        $self->{builder}->get_object( 'CustomLogs_label' )->set_markup( "<span color='blue'><b>Custom Logs</b></span>" );
        $custom_logs_box->pack_end( $notebook , TRUE , TRUE , 0 );
        $custom_logs_box->show_all;
    } else {
        $self->{builder}->get_object( 'CustomLogs_label' )->set_markup( "<b>Custom Logs</b>" );
    }

}

sub on_complete_filter_changed {
    
    my ( $self, $widget ) = @_;
    
    if ( ! $widget->get_active ) {
        return;
    }
    
    $self->get_completed_batches();
    
}

sub on_FetchJobLog_clicked {
    
    my $self = shift;

    my $job_id   = $self->{jobs}->get_column_value( "JOB_ID" );

    my $log_record = $self->{globals}->{config_manager}->sdf_connection( "LOG" )->select(
        "select * from job_full_log where job_id = ?"
      , [ $job_id ]
    );

    if ( @{$log_record} ) {
        my $launcher    = $self->open_window( 'window::framework_launcher', $self->{globals} );
        $launcher->log_to_buffer( $$log_record[0]->{LOG_TEXT} );
        $launcher->zoom_log();
    } else {
        $self->dialog(
            {
                title    => "No detail log in DB"
              , type     => "error"
              , markup   => "<i>There is no detail log in the database for the job.</i>\n\nThe <b>batch</b> process ( that forks jobs ) is responsible for"
                          . " copying text logs into the database. Something probably happened to the batch ( ie it was killed ). The detail log will still"
                          . " be available on the server this job ran on."
            }
        );
    }
    
}

sub on_FailJob_clicked {
    
    my $self = shift;
    
    my $job_id = $self->{jobs}->get_column_value( "JOB_ID" );
    
    $self->{jobs}->{dbh}->do( "update job_ctl set status = 'ERROR' where job_id = ?", [ $job_id ] );
    $self->{jobs}->query;
    
}

sub on_SetJobStatus_clicked {
    
    my $self = shift;
    
    my $job_id = $self->{jobs}->get_column_value( "JOB_ID" );
    my $status = $self->{jobs}->get_column_value( "STATUS" );
    
    my $status_response = $self->dialog(
        {
            title       => "Enter a new status"
          , type        => "input"
          , default     => $status
        }
    );
    
    if ( $status_response eq '' ) {
        
        return;
        
    } else {
        
        $self->{jobs}->{dbh}->do( "update job_ctl set status = ? where job_id = ?", [ $status_response, $job_id ] );
        $self->{jobs}->query;
        
    }
    
}

sub on_FailBatch_clicked {
    
    my $self = shift;
    
    my $batch_id = $self->{active_batches}->get_column_value( "BATCH_ID" );
    
    $self->{active_batches}->{dbh}->do( "update batch_ctl set status = 'COMPLETE_WITH_ERROR' where BATCH_ID = ?", [ $batch_id ] );
    $self->{jobs}->{dbh}->do( "update job_ctl set status = 'ERROR' where batch_id = ? and status = 'RUNNING'", [ $batch_id ] );
    $self->{active_batches}->query;
    $self->{complete_batches}->query;
    $self->{jobs}->query;
    
}

sub on_StartAutoRefresh_clicked {
    
    my $self = shift;
    
    my $timeout_seconds = $self->{builder}->get_object( 'AutorefreshTimeout' )->get_text;
    
    if ( ( $timeout_seconds !~ /^-?([\d]*)$/ )
      || ( ! $timeout_seconds )
    ) {
        return;
    }
    
    $self->{TIMEOUTS_CONTINUE} = TRUE;
    
    # The timer to actually refresh things
    $self->{refresh_timer} = Glib::Timeout->add( ( $timeout_seconds * 1000 ), sub { $self->refresh } );
    
    # A timer to push the progress bar along
    $self->{pulse_amount} = 1 / $timeout_seconds;
    $self->{progress_timer} = Glib::Timeout->add( 1000, sub { $self->kick_progress( $timeout_seconds ) } );
    
}

sub refresh {
    
    my $self = shift;
    
    $self->{jobs}->query;
    $self->{steps}->query;
    $self->{active_batches}->query;
    $self->{complete_batches}->query;
    $self->{datacop_totals}->query;
    $self->{datacop_issues_summary}->query;
    
    return $self->{TIMEOUTS_CONTINUE};
    
}

sub kick_progress {
    
    my $self = shift;    
    
    $self->pulse;
    
    return $self->{TIMEOUTS_CONTINUE};
    
}

sub on_StopAutoRefresh_clicked {
    
    my $self = shift;
    
    $self->{TIMEOUTS_CONTINUE} = FALSE;
    
}

sub on_ETL_Monitor_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;