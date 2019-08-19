package window::dashboard_mysql;

use parent 'window';

use strict;
use warnings;

use Glib qw( TRUE FALSE );

# These are columns in the MODEL. Note that you don't have to have a 1:1 relationship between model columns and treeview columns

use constant    LOCKS_COLUMN_TYPE            => 0;      # hidden column, stores the type ( eg locking, locked )
use constant    LOCKS_COLUMN_QUERY_ID        => 1;      # the query id
use constant    LOCKS_COLUMN_USER            => 2;
use constant    LOCKS_COLUMN_TIME_INFO       => 3;      # the time info column
use constant    LOCKS_COLUMN_SQL             => 4;      # the query SQL

# These are our types ( currently locking or locked )
use constant    TYPE_LOCKING                 => 'locking';
use constant    TYPE_LOCKED                  => 'blocked';
use constant    TYPE_EXECUTING               => 'executing';

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "main"
    );
    
    $self->maximize;
    
    $self->{builder}->connect_signals( undef, $self );
    
    $self->{target_chooser} = widget::conn_db_table_chooser->new(
        $self->{globals}
      , $self->{builder}->get_object( 'Connection_hbox' )
      , {
            database    => 0
          , schema      => 0
          , table       => 0
        }
      , {
            on_connection_changed         => sub { $self->on_connection_changed }
        }
    );
    
    # Set up the locks treeview
    my $treeview = $self->{locks_treeview} = $self->{builder}->get_object( 'Locks_treeview' );
    
    # The type icon:
    my $renderer = Gtk3::CellRendererPixbuf->new;
    
    my $icon_column = Gtk3::TreeViewColumn->new_with_attributes(
        ""
      , $renderer
    );
    
    $icon_column->set_cell_data_func( $renderer, sub { $self->render_locks_pixbuf_cell( @_ ); } );
    
    $treeview->append_column( $icon_column );
    
    # The query ID:
    my $query_id_renderer = Gtk3::CellRendererText->new;
    
    #$query_id_renderer->set( 'scale', 0.9 );
    
    $self->{QUERY_ID_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
        "Query ID"
      , $query_id_renderer
      , 'text'  => LOCKS_COLUMN_QUERY_ID
    );
    
    $treeview->append_column( $self->{QUERY_ID_COLUMN} );
    
    # The User:
    my $user_renderer = Gtk3::CellRendererText->new;
    
    #$user_id_renderer->set( 'scale', 0.9 );
    
    $self->{USER_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
        "User / DB"
      , $user_renderer
      , 'text'  => LOCKS_COLUMN_USER
    );
    
    $treeview->append_column( $self->{USER_COLUMN} );
    
    # The Time Info ID:
    my $time_info_renderer = Gtk3::CellRendererText->new;
    
    #$query_id_renderer->set( 'scale', 0.9 );
    
    $self->{TIME_INFO_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
        "Time Info"
      , $time_info_renderer
      , 'text'  => LOCKS_COLUMN_TIME_INFO
    );
    
    $treeview->append_column( $self->{TIME_INFO_COLUMN} );
    
    # The SQL:
    my $sql_renderer = Gtk3::CellRendererText->new;
    
    #$sql_renderer->set( 'scale', 0.9 );
    
    $self->{QUERY_ID_COLUMN} = Gtk3::TreeViewColumn->new_with_attributes(
        "SQL"
      , $sql_renderer
      , 'text'  => LOCKS_COLUMN_SQL
    );
    
    $sql_renderer->set( 'wrap-width' , 10 );
    $sql_renderer->set( 'wrap-mode'  , 'PANGO_WRAP_WORD' );
    $sql_renderer->set_fixed_height_from_font( 3 );
    
    $treeview->append_column( $self->{QUERY_ID_COLUMN} );
    
    my $icon_folder = $self->{globals}->{paths}->{app} . "/icons";
    
    $self->{icons}->{locking}               = $self->to_pixbuf( $icon_folder . "/locking_32x32.png" );
    $self->{icons}->{blocked}               = $self->to_pixbuf( $icon_folder . "/blocked_32x32.png" );
    $self->{icons}->{executing}             = $self->to_pixbuf( $icon_folder . "/executing_32x32.png" );
    
    return $self;
    
}

sub on_connection_changed {
    
    my ( $self ) = @_;
    
    $self->refresh_locks();
    
    $self->{locks_refreshing} = TRUE;
    
    # The timer to actually refresh things
    $self->{locks_refresh_timer} = Glib::Timeout->add( ( 2 * 1000 ), sub { $self->do_auto_refresh() } );
    
}

sub refresh_locks {
    
    my ( $self ) = @_;
    
    my $model = $self->{locks_model} = Gtk3::TreeStore->new(
        qw ' Glib::String Glib::String Glib::String Glib::String Glib::String '
    );
    
    my $dbh = $self->{target_chooser}->get_db_connection();
    
    # Fetch locks
    my $locks_metadata = $self->{locks_metadata} = $dbh->select(
        "select * from sys.innodb_lock_waits order by wait_started"
      , undef
      , undef
    );
    
    my $active_queries = $self->{active_queries} = $dbh->select(
        "select ID , USER, DB, STATE , INFO, COMMAND, TIME from information_schema.processlist order by TIME desc"
      , undef
      , "ID"
    );
    
    # Build a hierarchy of blocking queries ...
    #  ... as ( in theory at least ) ... some queries that are blocking
    # could themselves be being blocked by other queries
    
    # Build a 2-deep hierarchy  ...
    
    my ( $locked_hash , $locking_hash );
    
    foreach my $lock ( @{$locks_metadata} ) {
        
        $locked_hash->{ $lock->{waiting_pid} } = {
            QUERY_ID          => $lock->{waiting_pid}
          , USER              => $active_queries->{ $lock->{blocking_pid} }->{USER}
          , SQL               => $active_queries->{ $lock->{blocking_pid} }->{INFO}
          , LOCKED_BY         => $lock->{blocking_pid}
          , WAIT_AGE_SECS     => $lock->{wait_age_secs}
        };
        
        $locking_hash->{ $lock->{blocking_pid} }->{BLOCKING_TRX_AGE} = $lock->{blocking_trx_age};
        push @{$locking_hash->{ $lock->{blocking_pid} }->{LOCKING}} , $lock->{waiting_pid};
        
    }
    
    $self->{rendered_pids} = {};
    $self->{locked_hash} = $locked_hash;
    
    my $completed_roots;
    
    # Now walk over all in our 2-deep hierarchy and put the whole thing together ...
    foreach my $waiting_pid ( keys %{$locked_hash} ) {
        
        my $grandparent_pid = $self->get_grandparent_pid( $locked_hash , $waiting_pid );
        
        if ( ! $completed_roots->{ $grandparent_pid } ) {
            $self->render_branch( $model , undef , $locking_hash , $grandparent_pid );
            $completed_roots->{ $grandparent_pid } = 1;
        }
        
    }
    
    # Now walk over the remaining active queries and render them ...
    foreach my $query_id ( keys %{$active_queries} ) {
        
        if ( exists $self->{rendered_pids}->{ $query_id } ) {
            next;
        }
        
        if ( $active_queries->{ $query_id }->{COMMAND} ne 'Sleep' ) {
            $self->render_branch( $model , undef , undef , $query_id );
        }
        
    }
    
    $self->{locks_treeview}->set_model( $model );
    
    $self->{locks_treeview}->expand_all;
    
}

sub render_branch {
    
    my ( $self , $model , $parent_iter , $locking_hash , $query_id ) = @_;
    
    my ( $this_iter , $time_info );
    
    if ( $parent_iter ) {
        $this_iter = $model->append( $parent_iter );
        $time_info = "Waiting [" . $self->{locked_hash}->{ $query_id }->{WAIT_AGE_SECS} . "] seconds";
    } elsif ( $locking_hash ) {
        $this_iter = $model->append;
        $time_info = "Transaction age: [" . $locking_hash->{ $query_id }->{BLOCKING_TRX_AGE} . "]";
    } else {
        $this_iter = $model->append;
        $time_info = "Executing for [" . $self->{active_queries}->{ $query_id }->{TIME} . "] seconds";
    }
    
    my $type;
    
    if ( $locking_hash ) {
        if ( exists $locking_hash->{ $query_id } ) {
            $type = TYPE_LOCKING;
        } else {
            $type = TYPE_LOCKED;
        }
    } else {
        $type = TYPE_EXECUTING;
    }
    
    $model->set(
        $this_iter
      , LOCKS_COLUMN_TYPE,       $type
      , LOCKS_COLUMN_QUERY_ID,   $query_id
      , LOCKS_COLUMN_USER,       $self->{active_queries}->{ $query_id }->{USER} . "\n" . $self->{active_queries}->{ $query_id }->{DB}
      , LOCKS_COLUMN_TIME_INFO,  $time_info
      , LOCKS_COLUMN_SQL,        $self->{active_queries}->{ $query_id }->{INFO}
    );
    
    $self->{rendered_pids}->{ $query_id } = 1;
    
    if ( $locking_hash ) {
        foreach my $child_id ( @{ $locking_hash->{ $query_id }->{LOCKING} } ) {
            $self->render_branch( $model , $this_iter , $locking_hash , $child_id );
        }
    }
    
}

sub get_grandparent_pid {
    
    my ( $self , $locked_hash , $waiting_pid ) = @_;
    
    my $this_item = $locked_hash->{ $waiting_pid };
    my $locked_by = $this_item->{LOCKED_BY};
    
    if ( exists $locked_hash->{ $locked_by } ) {
        return $self->get_grandparent_pid( $locked_hash , $locked_by );
    } else {
        return $locked_by;
    }
    
}

sub render_locks_pixbuf_cell {
    
    my ( $self, $tree_column, $renderer, $model, $iter ) = @_;
    
    my $type = $model->get( $iter, LOCKS_COLUMN_TYPE );
    
    if ( $type ) {
        $renderer->set( pixbuf => $self->{icons}->{ $type } );
    } else {
        $renderer->set( pixbuf => undef );
    }
    
    return FALSE;

}

sub on_Start_Autorefresh_clicked {
    
    my $self = shift;
    
    my $timeout_seconds =
        ( $self->dialog(
            {
                title       => "Timing ..."
              , text        => "Refresh once every <how many> seconds?"
              , type        => "input"
              , default     => 5
            }
        ) ) || 5;
    
    if ( $timeout_seconds !~ /^-?([\d]*)$/ ) {
        $timeout_seconds = 5;
    }
    
    print "Starting autorefresh ...\n";
    
    $self->{locks_refreshing} = TRUE;
    
    # The timer to actually refresh things
    $self->{locks_refresh_timer} = Glib::Timeout->add( ( $timeout_seconds * 1000 ), sub { $self->do_auto_refresh() } );
    
}

sub on_Stop_Autorefresh_clicked {
    
    my $self = shift;
    
    $self->{locks_refreshing} = FALSE;
    
}

sub do_auto_refresh {
    
    my ( $self ) = @_;
    
    if ( $self->{locks_refreshing} ) {
        $self->refresh_locks();
        return TRUE;
    } else {
        return FALSE;
    }
    
}

sub get_selected_iters {
    
    my ( $self , $treeview ) = @_;
    
    my ( $selected_paths, $model ) = $treeview->get_selection->get_selected_rows;
    
    my @iters;
    
    foreach my $selected_path ( @{$selected_paths} ) {
        push @iters, $model->get_iter( $selected_path );
    }
    
    return $model, \@iters;
    
}

sub on_Locks_Kill_Connection_clicked {
    
    my $self = shift;
    
    my $display = Gtk3::Gdk::Display::get_default;
    $display->beep();
    
    my ( $model , $iters ) = $self->get_selected_iters( $self->{locks_treeview} );
    
    my $dbh = $self->{target_chooser}->get_db_connection();
    
    foreach my $iter ( @{$iters} ) {
        
        my $query_id = $model->get( $iter , LOCKS_COLUMN_QUERY_ID );
        
        $dbh->do( $dbh->generate_session_kill_sql( $query_id ) );
        
    }
    
    $self->refresh_locks();
    
}

sub on_main_destroy {
    
    my $self = shift;
    
    $self->{locks_refreshing} = FALSE;
    $self->close_window();
    
}

1;
