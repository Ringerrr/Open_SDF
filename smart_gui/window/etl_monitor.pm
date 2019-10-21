package window::etl_monitor;

use parent 'window';

use strict;
use warnings;

use Glib qw( TRUE FALSE );

use Data::Dumper;

use feature 'switch';

use constant    STATUS_PIXBUF_COLUMN           => 0;
use constant    ID_NO_COLUMN                   => 1;
use constant    ID_TEXT_COLUMN                 => 2;
use constant    SECS_COLUMN                    => 3;
use constant    TYPE_COLUMN                    => 4;

use constant    STEPS_LOAD_EXECUTION_ID_COLUMN => 0;
use constant    STEPS_WARNINGS_COLUMN          => 1;
use constant    STEPS_TEMPLATE_COLUMN          => 2;
use constant    STEPS_TARGET_COLUMN            => 3;
use constant    STEPS_SECONDS_COLUMN           => 4;
use constant    STEPS_ROWS_COLUMN              => 5;
use constant    STEPS_PERF_STATS               => 6;

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

    $self->{icons}->{COMPLETE}              = $self->to_pixbuf( $self->get_icon_path( "/monitor/tick_16x16.png" ) );
    $self->{icons}->{COMPLETE_WITH_ERROR}   = $self->to_pixbuf( $self->get_icon_path( "/monitor/error_16x16.png" ) );
    $self->{icons}->{ERROR}                 = $self->{icons}->{COMPLETE_WITH_ERROR};
    $self->{icons}->{RUNNING}               = $self->to_pixbuf( $self->get_icon_path( "/monitor/running_16x16.png" ) );
    $self->{icons}->{UNKNOWN}               = $self->to_pixbuf( $self->get_icon_path( "/monitor/unknown_16x16.png" ) );

    # For icon menus, we need Gtk3::Images instead of Pixbufs. Whatever.
    $self->{images}->{COMPLETE_WITH_ERROR}  = Gtk3::Image->new_from_pixbuf( $self->{icons}->{COMPLETE_WITH_ERROR} );
    $self->{images}->{FETCH}                = Gtk3::Image->new_from_pixbuf( $self->to_pixbuf( $self->get_icon_path( "/monitor/fetch_16x16.png" ) ) );

    $self->build_tree();
    $self->build_steps_view();
    $self->build_steps_model();

    # Syntax highlighting
    my $view_buffer = Gtk3::SourceView::Buffer->new_with_language( $self->{globals}->{gtksourceview_language} );
    $view_buffer->set_highlight_syntax( TRUE );
    
    $view_buffer->set_style_scheme( $self->{globals}->{gtksourceview_scheme} );
    
    my $source_view = $self->{builder}->get_object( 'executed_template' );
    
    if ( Gtk3::MINOR_VERSION >= 16 ) {
        $source_view->set_background_pattern( 'GTK_SOURCE_BACKGROUND_PATTERN_TYPE_GRID' );
    }
    
    $source_view->set_buffer( $view_buffer );

    $self->{log_dbh} = $self->{globals}->{config_manager}->sdf_connection( "LOG" );

    if ( $batch_id ) {
        $self->{builder}->get_object( "BatchID" )->set_text( $batch_id );
        $self->{builder}->get_object( "BatchID_Filter" )->set_active( 1 );
    }

    $self->get_batches_and_jobs();
    
    $self->{datacop_totals} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $self->{globals}->{config_manager}->sdf_connection( "LOG" )
      , read_only               => 1
      #, primary_keys            => [ "BATCH_ID" ]
      , auto_incrementing       => 0
      , column_sorting          => 1
      , force_upper_case_fields => 1
      , sql                     => {
                                            select          => "records_validated, records_rejected"
                                          , from            => "data_cop_totals"
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
                                            select          => "column_name , column_type , column_type_extra , issue_type , issue_count"
                                          , from            => "data_cop_issues_summary"
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
        
        $filter = "where B.batch_id in ( " . $self->{builder}->get_object( "BatchID" )->get_text . " )";
        
    } elsif ( $self->{builder}->get_object( "Today" )->get_active ) {
        
        $filter = "where  B.status in ( 'COMPLETE', 'COMPLETE_WITH_ERROR' )\n"
                . "and now() - interval '1 days' <= B.START_TS";
        
    } elsif ( $self->{builder}->get_object( "ThisWeek" )->get_active ) {
        
        $filter = "where  B.status in ( 'COMPLETE', 'COMPLETE_WITH_ERROR' )\n"
                . "and now() - interval '7 days' <= B.START_TS";
        
    } else {
        
        $filter = "where  B.status in ( 'COMPLETE', 'COMPLETE_WITH_ERROR' )\n"
        
    }
    
    my $sql = "select B.batch_id , B.batch_identifier , B.start_ts , B.end_ts , B.status , B.processing_time , B.hostname\n"
            . "from   batch_ctl B left join job_ctl J on B.batch_id = J.batch_id\n"
            . "$filter\n"
            . "group by B.batch_id , B.batch_identifier , B.start_ts , B.end_ts , B.status , B.processing_time , B.hostname";
    
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
                name    => "batch_id"
              , type    => "number"
            }
          , {
                name    => "batch_identifier"
              , type    => "number"
            }
#          , {
#                name    => "PROCESS_GRP_NME"
#              , type    => "text"
#            }
          , {
                name    => "start_ts"
              , type    => "text"
            }
          , {
                name    => "end_ts"
              , type    => "text"
            }
          , {
                name    => "status"
              , type    => "text"
            }
          , {
                name    => "processing_time"
              , type    => "number"
            }
          , {
                name    => "hostname"
              , type    => "text"
            }
        ]
      , $self->{mem_dbh}
      , "complete_batches"
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

    $self->get_batches_and_jobs();

}

sub get_batches_and_jobs {

    my $self = shift;

    my $filter;

    if ( $self->{builder}->get_object( "BatchID_Filter" )->get_active ) {

        $filter = "where B.batch_id in ( " . $self->{builder}->get_object( "BatchID" )->get_text . " )";

    } elsif ( $self->{builder}->get_object( "Today" )->get_active ) {

        $filter = "where now() - interval '1 days' <= B.START_TS";

    } elsif ( $self->{builder}->get_object( "ThisWeek" )->get_active ) {

        $filter = "where now() - interval '7 days' <= B.START_TS";

    } else {

        $filter = "where 1=1\n"

    }

    my $sql = "select B.batch_id , B.batch_identifier             , B.start_ts as batch_start_ts , B.end_ts as batch_end_ts , B.status as batch_status , B.processing_time as batch_processing_time\n"
            . "     , J.job_id   , J.identifier as job_identifier , J.start_ts as job_start_ts   , J.end_ts as job_end_ts   , J.status as job_status   , J.processing_time as job_processing_time\n"
            . "from   batch_ctl B left join job_ctl J on B.batch_id = J.batch_id\n"
            . "$filter\n";

    print "\n\n$sql\n\n";

    my $dbh = $self->{globals}->{config_manager}->sdf_connection( "LOG" );

    my $batches_and_jobs = $dbh->select( $sql );

    my $batches_updated;

    foreach my $rec ( @{$batches_and_jobs} ) {

        my $batch_iter = $self->{model}->get_iter_first;
        my ( $batch_found , $job_found );

        while ( $batch_iter ) {

            # Search for the batch.
            # iter_next() searches at the *current* *level, so this will only reveal batches for us

            if ( $self->{model}->get( $batch_iter, ID_NO_COLUMN ) == $rec->{batch_id} ) {
                # We found this batch in the tree. Update it and jobs underneath it
                if ( ! $batches_updated->{ $rec->{batch_id} } ) { # Only update the batch once per requery
                    $self->{model}->set( $batch_iter , STATUS_PIXBUF_COLUMN , $rec->{batch_status} );
                    $batches_updated->{ $rec->{batch_id} } = 1;
                }
                $self->update_job_in_tree( $batch_iter , $rec );
                last;
            }

            if ( ! $self->{model}->iter_next( $batch_iter ) ) {
                last;
            }

        }

        if ( ! $batches_updated->{ $rec->{batch_id} } ) {
            $batch_iter = $self->{model}->append();
            $self->{model}->set(
                $batch_iter
              , STATUS_PIXBUF_COLUMN , $rec->{batch_status}
              , ID_NO_COLUMN         , $rec->{batch_id}
              , ID_TEXT_COLUMN       , $rec->{batch_identifier}
              , SECS_COLUMN          , $rec->{batch_processing_time}
              , TYPE_COLUMN          , 'BATCH'
            );
            $self->update_job_in_tree( $batch_iter , $rec );
        }

    }

    $self->{treeview}->expand_all;

}

sub update_job_in_tree {

    my ( $self , $batch_iter , $rec ) = @_;

    my $job_iter = $self->{model}->iter_children( $batch_iter );
    my $job_found;

    while ( $job_iter ) {

        # Search for the job.
        # iter_next() searches at the *current* *level, so this will only reveal jobs for us

        if ( $self->{model}->get( $job_iter, ID_NO_COLUMN ) == $rec->{job_id} ) {
            # We found this job in the tree. Update it.
            $self->{model}->set( $job_iter , STATUS_PIXBUF_COLUMN , $rec->{job_status} );
            $self->{model}->set( $job_iter , SECS_COLUMN          , $rec->{job_processing_time} );
            $job_found = 1;
            last;
        }

        if ( ! $self->{model}->iter_next( $job_iter ) ) {
            last;
        }

    }

    if ( ! $job_found ) {
        $job_iter = $self->{model}->append( $batch_iter );
        $self->{model}->set(
            $job_iter
          , STATUS_PIXBUF_COLUMN , $rec->{job_status}
          , ID_NO_COLUMN         , $rec->{job_id}
          , ID_TEXT_COLUMN       , $rec->{job_identifier}
          , SECS_COLUMN          , $rec->{job_processing_time}
          , TYPE_COLUMN          , 'JOB'
        );
    }

}

sub get_steps {

    my $self = shift;

    my $steps = $self->{log_dbh}->select(
        qq
{select
    load_execution_id
  , warnings
  , template_name
  , coalesce( target_db_name, '' ) || '.' || coalesce( target_schema_name, '' ) || '.' || coalesce( target_table_name, '' ) as target
  , processing_time
  , rows_affected
  , perf_stats
from
    load_execution
where
    job_id = ?
order by sequence_order}
      , [ $self->{job_id} ]
    );

    foreach my $step ( @{$steps} ) {

        my $iter = $self->{steps_model}->get_iter_first;
        my $step_found;

        while ( $iter ) {
            # Search for the step.
            if ( $self->{steps_model}->get( $iter, STEPS_LOAD_EXECUTION_ID_COLUMN ) == $step->{load_execution_id} ) {
                # We found this step. Update it.
                $self->{steps_model}->set( $iter , STEPS_WARNINGS_COLUMN      , $step->{warnings}
                                                 , STEPS_TEMPLATE_COLUMN      , $step->{template_name}
                                                 , STEPS_TARGET_COLUMN        , $step->{target}
                                                 , STEPS_SECONDS_COLUMN       , $step->{processing_time}
                                                 , STEPS_ROWS_COLUMN          , $step->{rows_affected}
                                                 , STEPS_PERF_STATS           , $step->{perf_stats} );
                $step_found = 1;
                last;
            }
            if ( ! $self->{steps_model}->iter_next( $iter ) ) {
                last;
            }
        }

        if ( ! $step_found ) {
            if ( ! $self->{icons}->{ $step->{template_name} } ) {
                my $icon_path = $self->get_template_icon_path( $step->{template_name} . ".png" );
                if ( $icon_path ) {
                    $self->{icons}->{ $step->{template_name} } = $self->to_pixbuf( $icon_path );
                }
            }
            $iter = $self->{steps_model}->append();
            $self->{steps_model}->set( $iter , STEPS_LOAD_EXECUTION_ID_COLUMN , $step->{load_execution_id}
                                             , STEPS_WARNINGS_COLUMN          , $step->{warnings}
                                             , STEPS_TEMPLATE_COLUMN          , $step->{template_name}
                                             , STEPS_TARGET_COLUMN            , $step->{target}
                                             , STEPS_SECONDS_COLUMN           , $step->{processing_time}
                                             , STEPS_ROWS_COLUMN              , $step->{rows_affected}
                                             , STEPS_PERF_STATS               , $step->{perf_stats} );
        }

    }

}

sub render_status_pixbuf_cell {

    my ( $self , $treeview_column , $cellrenderer_pixbif , $liststore , $iter , $something , $column_name ) = @_;

    my $status_text = $liststore->get( $iter, STATUS_PIXBUF_COLUMN );

    no warnings 'uninitialized';

    if ( defined $status_text && $status_text ne '' ) {
        if ( exists $self->{icons}->{ $status_text } ) {
            $cellrenderer_pixbif->set( pixbuf => $self->{icons}->{ $status_text } );
        } else {
            $cellrenderer_pixbif->set( pixbuf => $self->{icons}->{UNKNOWN} );
        }
    } else {
        $cellrenderer_pixbif->set( pixbuf => undef );
    }

    return FALSE;

}

sub delete_batch {
    
    my $self = shift;
    
    my $batch_id = $self->{complete_batches}->get_column_value( "batch_id" );
    
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

sub on_steps_row_select {
    
    my ( $self, $tree_selection ) = @_;

    my ( $model, $iters ) = $self->get_selected_iters( $self->{steps_view} );

    foreach my $iter ( @{$iters} ) {

        my $le_id = $self->{steps_model}->get( $iter , STEPS_LOAD_EXECUTION_ID_COLUMN );

        my $log_db = $self->{globals}->{config_manager}->sdf_connection( "LOG" );

        my $log_stuff;

        # The standard log types - log and error ...
        foreach my $type ( "log", "error" ) {

            my $sth = $log_db->prepare(
                "select part_text from execution_log_parts\n"
              . "where load_execution_id= ? and log_type = ?\n"
              . "order by part_sequence"
            ) || die( $log_db->errstr );

            $sth->execute( $le_id, $type )
                || die( $sth->errstr );

            while ( my $row = $sth->fetchrow_arrayref ) {
                $log_stuff->{ $type } .= $$row[0];
            }

            $sth->finish();

        }

        $self->{builder}->get_object( "perf_stats" )->get_buffer->set_text( $self->{steps_model}->get( $iter , STEPS_PERF_STATS ) || '' );
        $self->{builder}->get_object( "executed_template" )->get_buffer->set_text( $log_stuff->{log} || '' );

        # Warnings
        my $warnings = $self->{steps_model}->get( $iter , STEPS_WARNINGS_COLUMN );
        my $page_focus_grab;

        if ( $warnings && $warnings ne "[]\n" ) {
            $self->{builder}->get_object( "warnings_msg" )->get_buffer->set_text( $warnings );
            $self->{builder}->get_object( "Warnings_label" )->set_markup( "<span color='red'><b>Warnings</b></span>" );
            my $focused_page = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();;
            if ( $focused_page != 3 && $focused_page !=4 ) {
                $self->{previous_active_page} = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();
            }
            $page_focus_grab = TRUE;
            $self->{builder}->get_object( 'ExecutionLog_notebook' )->set_current_page( 3 );
        } else {
            $self->{builder}->get_object( "warnings_msg" )->get_buffer->set_text( '' );
            $self->{builder}->get_object( "Warnings_label" )->set_markup( "<b>Warnings</b>" );
        }

        if ( $log_stuff->{error} ) {
            $self->{builder}->get_object( "error_msg" )->get_buffer->set_text( $log_stuff->{error} );
            $self->{builder}->get_object( "Errors_label" )->set_markup( "<span color='red'><b>Errors</b></span>" );
            if ( ! $page_focus_grab ) {
                my $focused_page = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();
                if ( $focused_page != 3 && $focused_page !=4 ) {
                    $self->{previous_active_page} = $self->{builder}->get_object( 'ExecutionLog_notebook' )->get_current_page();
                }
            }
            $self->{builder}->get_object( 'ExecutionLog_notebook' )->set_current_page( 4 );
            $page_focus_grab = TRUE;
        } else {
            $self->{builder}->get_object( "error_msg" )->get_buffer->set_text( '' );
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
            "select log_type from execution_log_parts\n"
          . "where load_execution_id = ? and log_type not in ( 'log' , 'error' )\n"
          . "group by log_type"
        ) || die( $log_db->errstr );

        $sth->execute( $le_id )
            || die( $sth->errstr );

        my $notebook;

        while ( my $row = $sth->fetchrow_hashref ) {

            my $custom_log_sth = $log_db->prepare(
                "select part_text from execution_log_parts\n"
               . "where load_execution_id = ? and log_type = ?\n"
               . "order by part_sequence"
            ) || die( $log_db->errstr );

            $custom_log_sth->execute( $le_id, $row->{log_type} )
                || die( $sth->errstr );

            my $custom_log_text;

            while ( my $custom_log_row = $custom_log_sth->fetchrow_hashref ) {
                $custom_log_text .= $custom_log_row->{part_text};
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
            $label->set_markup( "<b>" . $row->{log_type} . "</b>" );

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

}

sub on_complete_filter_changed {
    
    my ( $self, $widget ) = @_;
    
    if ( ! $widget->get_active ) {
        return;
    }
    
    $self->get_completed_batches();
    
}

sub fetch_log {
    
    my $self = shift;

    my $log_record = $self->{globals}->{config_manager}->sdf_connection( "LOG" )->select(
        "select * from job_full_log where job_id = ?"
      , [ $self->{job_id} ]
    );

    if ( @{$log_record} ) {
        my $launcher    = $self->open_window( 'window::framework_launcher', $self->{globals} );
        $launcher->log_to_buffer( $$log_record[0]->{log_text} );
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

sub fail_job {
    
    my $self = shift;

    my $dbh = $self->{globals}->{config_manager}->sdf_connection( "LOG" );

    $dbh->do( "update job_ctl set status = 'ERROR' where job_id = ?", [ $self->{job_id} ] );
    
}

sub on_SetJobStatus_clicked { # TODO: not hooked up?
    
    my $self = shift;
    
    my $job_id = $self->{jobs}->get_column_value( "job_id" );
    my $status = $self->{jobs}->get_column_value( "status" );
    
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

sub fail_batch {
    
    my $self = shift;

    my ( $model, $selected_path, $iter, $type, $id ) = $self->get_clicked_tree_object( $self->{treeview} );

    my $dbh = $self->{globals}->{config_manager}->sdf_connection( "LOG" );

    $dbh->do( "update batch_ctl set status = 'COMPLETE_WITH_ERROR' where batch_id = ?", [ $id ] );
    $dbh->do( "update job_ctl set status = 'ERROR' where batch_id = ? and status = 'RUNNING'", [ $id ] );
    
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
    
}

sub refresh {
    
    my $self = shift;

    $self->get_batches_and_jobs();
    $self->get_steps();
    $self->{datacop_totals}->query;
    $self->{datacop_issues_summary}->query;
    $self->pulse();

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

sub build_tree {

    my $self = shift;

    if ( ! $self->{treeview} ) {

        $self->{model} = Gtk3::TreeStore->new(
            qw' Glib::String Glib::Int Glib::String Glib::Int Glib::String '
        );

        $self->{treeview} = $self->{builder}->get_object( 'batch_job_tree' );

        my $renderer = Gtk3::CellRendererPixbuf->new;

        my $column = Gtk3::TreeViewColumn->new_with_attributes(
            ""
          , $renderer
        );

        $column->set_cell_data_func( $renderer, sub { $self->render_status_pixbuf_cell( @_ ); } );

        $self->{treeview}->append_column( $column );

        # 1st visible column - ID
        my $col_1_renderer = Gtk3::CellRendererText->new;
        $col_1_renderer->set( xalign      => 1 );
        $col_1_renderer->set( 'scale', 0.9 );

        # TODO: scaling of text and icons based on config?
        #$col_1_renderer->set( 'scale', 0.9 );

        $self->{ID_NO_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
            "id #",
            $col_1_renderer,
            'text'  => ID_NO_COLUMN
        );

        $self->{treeview}->append_column( $self->{ID_NO_COLUMN} );

        $self->{ID_NO_COLUMN}->{_percent} = 10;
        $self->{ID_NO_COLUMN}->{_renderer} = $col_1_renderer;
        $self->{ID_NO_COLUMN}->set_sort_column_id( ID_NO_COLUMN );

        # 2nd visible column - ID text
        my $col_2_renderer = Gtk3::CellRendererText->new;

        # TODO: scaling of text and icons based on config?
        $col_2_renderer->set( 'scale', 0.9 );

        $self->{ID_TEXT_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
            "identifier",
            $col_2_renderer,
            'text'  => ID_TEXT_COLUMN
        );

        $self->{treeview}->append_column( $self->{ID_TEXT_COLUMN} );

        $self->{ID_TEXT_COLUMN}->{_percent} = 75;
        $self->{ID_TEXT_COLUMN}->{_renderer} = $col_2_renderer;
        $self->{ID_TEXT_COLUMN}->set_sort_column_id( ID_TEXT_COLUMN );

        # 3rd visible column - seconds
        my $col_3_renderer = Gtk3::CellRendererText->new;
        $col_3_renderer->set( xalign      => 1 );

        $self->{SECS_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
            "secs",
            $col_3_renderer,
            'text'  => SECS_COLUMN
        );

        $self->{treeview}->append_column( $self->{SECS_COLUMN} );

        $self->{SECS_COLUMN}->{_percent} = 5;
        $self->{SECS_COLUMN}->{_renderer} = $col_3_renderer;
        $self->{SECS_COLUMN}->set_sort_column_id( SECS_COLUMN );

        # $self->{treeview}->signal_connect( 'key_press_event'      => sub { $self->on_tree_key_press_event( @_ ) } );

        $self->{treeview}->set_model( $self->{model} );
        $self->{treeview}->get_selection->signal_connect( changed  => sub { $self->on_tree_row_select( @_ ); } );
        $self->{treeview}->signal_connect( 'button_press_event'   => sub { $self->on_tree_click( @_ ) } );

        $self->{ID_NO_COLUMN}->set_sort_order( 'GTK_SORT_DESCENDING' );

    }

}

sub build_steps_model {

    my $self = shift;

    $self->{steps_model} = Gtk3::ListStore->new(
        qw' Glib::String Glib::String Glib::String Glib::String Glib::Int Glib::Int Glib::String '
    );

    $self->{steps_view}->set_model( $self->{steps_model} );

}

sub build_steps_view {

    my $self = shift;

    if ( ! $self->{steps_model} ) {

        $self->{steps_view} = $self->{builder}->get_object( 'Steps' );

        my $renderer = Gtk3::CellRendererPixbuf->new;

        my $column = Gtk3::TreeViewColumn->new_with_attributes(
            ""
          , $renderer
        );

        $column->set_cell_data_func( $renderer, sub { $self->render_template_icon( @_ ); } );

        $self->{steps_view}->append_column( $column );

        # 1st visible column - Template
        my $col_1_renderer = Gtk3::CellRendererText->new;
        $col_1_renderer->set( 'scale', 0.8 );

        $self->{TEMPLATE_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
            "Template",
            $col_1_renderer,
            'text'  => STEPS_TEMPLATE_COLUMN
        );

        $self->{steps_view}->append_column( $self->{TEMPLATE_COLUMN} );

        $self->{TEMPLATE_COLUMN}->{_percent} = 30;
        $self->{TEMPLATE_COLUMN}->{_renderer} = $col_1_renderer;
        $self->{TEMPLATE_COLUMN}->set_sort_column_id( STEPS_TEMPLATE_COLUMN );

        # 2nd visible column - target
        my $col_2_renderer = Gtk3::CellRendererText->new;
        $col_2_renderer->set( 'scale', 0.8 );

        $self->{TARGET_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
            "Target",
            $col_2_renderer,
            'text'  => STEPS_TARGET_COLUMN
        );

        $self->{steps_view}->append_column( $self->{TARGET_COLUMN} );

        $self->{TARGET_COLUMN}->{_percent} = 50;
        $self->{TARGET_COLUMN}->{_renderer} = $col_2_renderer;
        $self->{TARGET_COLUMN}->set_sort_column_id( STEPS_TARGET_COLUMN );

        # 3rd visible column - secs
        my $col_3_renderer = Gtk3::CellRendererText->new;
        $col_3_renderer->set( xalign      => 1 );

        $self->{STEPS_SECONDS_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
            "secs",
            $col_3_renderer,
            'text'  => STEPS_SECONDS_COLUMN
        );

        $self->{steps_view}->append_column( $self->{STEPS_SECONDS_COLUMN} );

        $self->{STEPS_SECONDS_COLUMN}->{_percent} = 8;
        $self->{STEPS_SECONDS_COLUMN}->{_renderer} = $col_3_renderer;
        $self->{STEPS_SECONDS_COLUMN}->set_sort_column_id( STEPS_SECONDS_COLUMN );

        # 4th visible column - secs
        my $col_4_renderer = Gtk3::CellRendererText->new;
        $col_4_renderer->set( xalign      => 1 );

        $self->{STEPS_ROWS_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
            "rows",
            $col_4_renderer,
            'text'  => STEPS_ROWS_COLUMN
        );

        $self->{steps_view}->append_column( $self->{STEPS_ROWS_COLUMN} );

        $self->{STEPS_ROWS_COLUMN}->{_percent} = 8;
        $self->{STEPS_ROWS_COLUMN}->{_renderer} = $col_4_renderer;
        $self->{STEPS_ROWS_COLUMN}->set_sort_column_id( STEPS_ROWS_COLUMN );

        $self->{steps_view}->get_selection->signal_connect( changed  => sub { $self->on_steps_row_select( @_ ); } );

    }

}

sub render_template_icon {

    my ( $self , $treeview_column , $cellrenderer_pixbif , $liststore , $iter , $something , $column_name ) = @_;

    my $template_name = $liststore->get( $iter, STEPS_TEMPLATE_COLUMN );

    no warnings 'uninitialized';

    if ( defined $template_name && $template_name ne '' ) {
        if ( exists $self->{icons}->{ $template_name } ) {
            $cellrenderer_pixbif->set( pixbuf => $self->{icons}->{ $template_name } );
        } else {
            $cellrenderer_pixbif->set( pixbuf => $self->{icons}->{UNKNOWN} );
        }
    } else {
        $cellrenderer_pixbif->set( pixbuf => undef );
    }

    return FALSE;

}

sub get_selected_iters {

    my ( $self , $view ) = @_;

    my ( $selected_paths, $model ) = $view->get_selection->get_selected_rows;

    my @iters;

    foreach my $selected_path ( @{$selected_paths} ) {
        push @iters, $model->get_iter( $selected_path );
    }

    return $model, \@iters;

}

sub on_tree_row_select {

    my ( $self, $tree_selection ) = @_;

    my ( $model, $iters ) = $self->get_selected_iters( $self->{treeview} );

    foreach my $iter ( @{$iters} ) {
        my $type = $model->get( $iter, TYPE_COLUMN );
        if ( $type eq 'JOB' ) {
            $self->{job_id} = $self->{model}->get( $iter , ID_NO_COLUMN );
            $self->build_steps_model();
            $self->get_steps();
            $self->{datacop_totals}->query( { bind_values => [ $self->{job_id} ] } );
            $self->{datacop_issues_summary}->query( { bind_values => [ $self->{job_id} ] } );
            $self->{builder}->get_object( "executed_template" )->get_buffer->set_text( '' );
            $self->{builder}->get_object( "error_msg" )->get_buffer->set_text( '' );
            # Also select the *last* step in the job ( the main reason we'd be in here is looking for errors, and
            # errors will typically be in the last step ...
            my $step_iter = $self->{steps_model}->get_iter_first;
            my $last_string;
            while ( $step_iter ) {
                $last_string = $self->{steps_model}->get_string_from_iter( $step_iter );
                if ( ! $self->{steps_model}->iter_next( $step_iter ) ) {
                    last;
                }
            }
            if ( defined $last_string ) {
                $self->{steps_view}->get_selection->select_iter( $self->{steps_model}->get_iter_from_string( $last_string ) );
            }
        }
    }

}

sub on_batch_job_tree_size_allocate {

    my ( $self, $widget, $rectangle ) = @_;

    if ( ! $self->{treeview_width} || $self->{treeview_width} != $rectangle->{width} ) { # TODO Remove on_size_allocate blocking workaround when blocking actually works

        $self->{treeview_width} = $rectangle->{width};

        foreach my $column_name ( "ID_NO_COLUMN" , "ID_TEXT_COLUMN" , "SECS_COLUMN" ) {

            my $column     = $self->{$column_name};
            my $this_width = $rectangle->{width} / 100 * $column->{_percent};

            print "$column_name width: $this_width\n";

            # TODO Figure out why we're getting very small values when constructing our own treeview
            # and avoid this some other way ... this works, but ... hmmmmm

            if ( $this_width < 1 ) {
                $this_width = 1;
            }

            Glib::Idle->add( sub {
                $column->set_fixed_width( $this_width );
                $column->{_renderer}->set( 'wrap-width', $this_width );
                return FALSE;
            } );

        }

    }

}

sub on_Steps_size_allocate {

    my ( $self, $widget, $rectangle ) = @_;

    if ( ! $self->{steps_view_width} || $self->{steps_view_width} != $rectangle->{width} ) { # TODO Remove on_size_allocate blocking workaround when blocking actually works

        $self->{steps_view_width} = $rectangle->{width};

        foreach my $column_name ( "TEMPLATE_COLUMN" , "TARGET_COLUMN" , "STEPS_SECONDS_COLUMN" , "STEPS_ROWS_COLUMN" ) {

            my $column     = $self->{$column_name};
            my $this_width = $rectangle->{width} / 100 * $column->{_percent};

            print "$column_name width: $this_width\n";

            # TODO Figure out why we're getting very small values when constructing our own treeview
            # and avoid this some other way ... this works, but ... hmmmmm

            if ( $this_width < 1 ) {
                $this_width = 1;
            }

            Glib::Idle->add( sub {
                $column->set_fixed_width( $this_width );
                $column->{_renderer}->set( 'wrap-width', $this_width );
                return FALSE;
            } );

        }

    }

}

sub get_clicked_tree_object {

    my ( $self, $widget ) = @_;

    my ( $selected_paths, $model ) = $widget->get_selection->get_selected_rows;

    my ( $selected_path, $iter, $type, $id );

    if ( $selected_paths && @{$selected_paths} ) {

        $selected_path  = $$selected_paths[0];
        $iter           = $model->get_iter( $selected_path );
        $type           = $model->get( $iter, TYPE_COLUMN );
        $id             = $model->get( $iter, ID_NO_COLUMN );

    }

    return ( $model, $selected_path, $iter, $type, $id );

}

sub on_tree_click {

    my ( $self, $widget, $event ) = @_;

    # The handlers for click events is split up to simplify things ...

    my $type    = $event->type;
    my $button  = $event->button;

    if ( $button == 3 ) {

        $self->build_context_menu( $widget, $event );

    } elsif ( $type eq '2button-press' ) {

        $self->handle_double_click( $widget, $event );

    }

}

sub build_context_menu {

    my ( $self, $widget, $event ) = @_;

    my ( $model, $selected_path, $iter, $type, $id ) = $self->get_clicked_tree_object( $widget );

    $self->{context_menu} = Gtk3::Menu->new;

    my $menu_control = {
        FAIL_BATCH        => {
                            text        => 'Fail Batch'
                          , method      => 'fail_batch'
                          , objects     => [ 'BATCH' ]
                          , icon        => 'COMPLETE_WITH_ERROR'
        }
      , FAIL_JOB     => {
                            text        => 'Fail Job'
                          , method      => 'fail_job'
                          , objects     => [ 'JOB' ]
                          , icon        => 'COMPLETE_WITH_ERROR'
        }
      , FETCH_LOG        => {
                            text        => 'Fetch Log'
                          , method      => 'fetch_log'
                          , objects     => [ 'JOB' ]
                          , icon        => 'FETCH'
        }
    };

    foreach my $key ( sort keys %{$menu_control} ) {

        my $this_menu_control = $menu_control->{$key};

        if ( grep { $_ eq $type } @{ $this_menu_control->{objects} } ) {

            my $item = Gtk3::ImageMenuItem->new_with_label( $this_menu_control->{text} );

            $item->set_image( $self->{images}->{ $this_menu_control->{icon} } );
            $item->set_always_show_image( TRUE );

            my $method_name = $this_menu_control->{method};
            $item->signal_connect_after( activate  => sub { $self->$method_name() } );
            $self->{context_menu}->append( $item );

            $item->show;

        }

    }

    $self->{context_menu}->popup( undef, undef, undef, undef, $event->button, $event->time );

    $self->{context_menu}->show;

    return FALSE;

}

sub on_ETL_Monitor_destroy {
    
    my $self = shift;
    
    $self->close_window();
    
}

1;
