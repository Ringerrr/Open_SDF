package window::main;

use parent 'window';

use strict;
use warnings;

use Glib qw( TRUE FALSE );

use JSON;
use File::Find::Rule;
use File::Path qw( make_path );
use Time::HiRes;

use tokens;

use constant    FLASH_TIMEOUT   => 500;

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->kick_gtk;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "main"
      , "image1",  "image11", "image4",  "image14"
      , "image15", "image17", "image22", "image2"               # you have to add all the images that exist 'outside' the window
      , "image24", "image3",  "image8",  "image25"
      , "image26"
    );
    
    my $pulse_items = 11; # If you add more items to the progress bar ->pulse() call, increment this number ...
    $self->{pulse_amount} = 1 / $pulse_items;
    
    $self->setup_header_bar;

    if ( $self->{globals}->{STDOUT_READER} ) {

        my $log_map = {
            LogTextView   => {
                                 filehandle => $self->{globals}->{STDOUT_READER}
                               , tag        => 'blue'
                             }
          , ErrorTextView => {
                                 filehandle => $self->{globals}->{STDERR_READER}
                               , tag        => 'red'
                             }
        };

        my $bold = Glib::Object::Introspection->convert_sv_to_enum("Pango::Weight", "bold");

        foreach my $view_type ( keys %{$log_map} ) {

            $self->{ $view_type } = $self->{builder}->get_object( $view_type )->get_buffer;
            $self->{ $view_type . "_vadjustment" } = $self->{builder}->get_object( $view_type . '_scrolled_window' )->get_vadjustment;

            foreach my $colour ( qw | red blue | ) {

                $self->{ $view_type }->create_tag(
                    $colour
                  , 'weight'    => $bold
                  , foreground  => $colour
                );

            }

            Glib::IO->add_watch( fileno( $log_map->{ $view_type }->{filehandle} ) , ['in'] , sub {

                my ( $fileno, $condition ) = @_;

                my $lines;

                sysread $log_map->{ $view_type }->{filehandle}, $lines, 65536;

                foreach my $line ( split /\n/, $lines ) {

                    $line .= "\n";

                    $self->{ $view_type }->insert_with_tags_by_name( $self->{ $view_type }->get_end_iter, $line, $log_map->{ $view_type }->{tag} );

                }

                Glib::Idle->add( sub {
                    $self->{ $view_type . "_vadjustment" }->set_value( $self->{  $view_type . "_vadjustment" }->get_upper - $self->{  $view_type . "_vadjustment" }->get_page_increment - $self->{  $view_type . "_vadjustment" }->get_step_increment );
                    return FALSE; # uninstall callback
                } );

                return TRUE;  # continue without uninstalling

            } );

        }

    }

    # Stack switcher for Job Config / Templates / Logs
    # We have to build this in code because other items in the header bar are added in code
    # and the order that we add them determines their position
    
    $self->{main_stack} = $self->{builder}->get_object( "main_stack" );
    $self->{main_stack_switcher} = Gtk3::StackSwitcher->new();
    $self->{main_stack_switcher}->set_stack( $self->{main_stack} );
    $self->{builder}->get_object( 'HeaderBar' )->pack_start( $self->{main_stack_switcher} );
    $self->{main_stack_switcher}->show_all;
    
    # Stack switcher for Groups / Configs / Harvest
    $self->{job_definition_stack} = $self->{builder}->get_object( "job_definition_stack" );
    $self->{job_definition_stack_switcher} = Gtk3::StackSwitcher->new();
    $self->{job_definition_stack_switcher}->set_stack( $self->{job_definition_stack} );
    $self->{builder}->get_object( 'HeaderBar' )->pack_end( $self->{job_definition_stack_switcher} );
    $self->{job_definition_stack_switcher}->show_all;
    
    $self->kick_gtk;
    
    # Build a connection to the control and log databases.
    # We require that users create a connection called METADATA for this purpose.
    
    my $values_hash = $self->{globals}->{config_manager}->get_auth_values( "METADATA" );

    my ( $control_db_connection , $log_db_connection );

    eval {
        
        $self->pulse( "Creating CONTROL connection" );

        $control_db_connection = $self->{globals}->{config_manager}->sdf_connection( "CONTROL" );
        $log_db_connection = $self->{globals}->{config_manager}->sdf_connection( "LOG" );

        # # TODO: unixODBC is segfaulting when we bind a parameter something larger than 32768 bytes
        # # in an insert statement. We're seeing this when logging relatively large queries to load_execution
        # # This ONLY affects the LOG connection, as other connections don't use bind variables - they execute
        # # plain SQL. Only logging uses bind variables.
        # # We should report this to IBM & unixODBC people ...
        #
        # $self->{globals}->{connections}->{CONTROL}->{connection}->{odbc_putdata_start} = 65536;
        
    };
    
    my $err = $@;
    
    if ( $err ) {
        
        print "\n\n$err";
        
        $self->dialog(
            {
                title       => "Couldn't create the minimum required connections"
              , type        => "error"
              , text        => "Please check your database connectivity in the configuration screen"
            }
        );
        
        $self->open_window( 'window::configuration', $self->{globals} );
        $self->{builder}->get_object( 'main' )->destroy;
        
        return undef;
        
    }
    
    $self->{builder}->connect_signals( undef, $self );
    
    $self->maximize;
    
    # Fetch tokens for autocompletion
    my $complex_tokens = tokens::COMPLEX_TOKENS;
    my $env_tokens     = tokens::ENV_TOKENS;
    
    @{$self->{_complex_tokens}} = keys %${complex_tokens};
    @{$self->{_env_tokens}}     = keys %${env_tokens};
    
    $self->pulse( "CONFIG form" );
    
    $self->{status_lbl} = $self->{builder}->get_object( "Status_lbl" );
    
    # The connections combo ...
    # We need to get a list of connection names from our local config DB,
    # and merge in any connections from the CONFIG table. This prevents
    # any users from corrupting data if they edit a record that has a connection
    # name that's not in their local config DB
    
    my $local_db_connections = $self->{globals}->{local_db}->select(
        "select ConnectionName from connections order by ConnectionName"
    );
    
    my $configurations_connections = $self->{globals}->{connections}->{CONTROL}->select(
        "select connection_name from config group by connection_name"
    );
    
    my $unique_connection_names;
    
    foreach my $this_conn ( @{$local_db_connections} ) {
        $unique_connection_names->{$this_conn->{ConnectionName}} = 1;
    }
    
    foreach my $this_conn ( @{$configurations_connections} ) {
        $unique_connection_names->{$this_conn->{connection_name}} = 1;
    }
    
    my $widget = $self->{builder}->get_object( "config.connection_name" );
    my $model = Gtk3::ListStore->new( "Glib::String" );
    
    foreach my $this_conn ( sort keys %{$unique_connection_names} ) {
        $model->set(
            $model->append
          , 0 , $this_conn
        );
    }
    
    # Add the special 'Memory' connection ...
    $model->set(
        $model->append
      , 0 , "Memory"
    );
    
#    $self->create_combo_renderers( $widget, 0 );
    
    $widget->set_model( $model );
    $widget->set_entry_text_column( 0 );
    
    # Set up GtkSourceView widgets with language specs and fixed-width fonts ...
    # The language specs provide syntax highlighting - see the gtksourceview directory
    
    for my $widget_name (
                          "param_value.param_value"
                        , "ParamDefaultValue"
                        , "template_text_read_only"
                        , "template.template_text"
    ) {
        
        my $view_buffer = Gtk3::SourceView::Buffer->new_with_language( $self->{globals}->{gtksourceview_language} );
        $view_buffer->set_highlight_syntax( TRUE );
        
        $view_buffer->set_style_scheme( $self->{globals}->{gtksourceview_scheme} );
        
        my $source_view = $self->{builder}->get_object( $widget_name );
        
        if ( &Gtk3::MINOR_VERSION >= 16 ) {
            $source_view->set_background_pattern( 'GTK_SOURCE_BACKGROUND_PATTERN_TYPE_GRID' );
        }

        $source_view->set_buffer( $view_buffer );
        
    }
    
    # When we have recordsets that are linked, we have to create all the *dependant* recordsets first ...

    $self->pulse( "Creating Gtk3::Ex::DBI::Form - {TEMPLATE}" );

    $self->update_template_selector;

    $self->{template} = Gtk3::Ex::DBI::Form->new(
        {
            dbh                     => $control_db_connection
          , sql                     => {
                                              select        => "*"
                                            , from          => "template"
            }
          , debug                   => TRUE
          , widget_prefix           => "template."
          , builder                 => $self->{builder}
          , on_current              => sub { $self->on_template_current( @_ ) }
          , before_apply            => sub { $self->before_template_apply( @_ ) }
          , on_apply                => sub { $self->on_template_apply( @_ ) }
          , on_delete               => sub { $self->on_template_delete( @_ ) }
          , recordset_tools_box     => $self->{builder}->get_object( "template_recordset_tools" )
          , recordset_tool_items    => [ "label" , "new", "undo", "delete", "apply", "clone", "package" ]
          , recordset_extra_tools   => {
                new => {
                         type        => 'button'
                       , markup      => "<span color='green'>insert</span>"
                       , icon_name   => 'document-new'
                       , coderef     => sub { $self->on_template_insert() }
                }
              , clone => {
                         type        => 'button'
                       , markup      => "<span color='purple'>clone</span>"
                       , icon_name   => "edit-copy"
                       , coderef     => sub { $self->template_copy() }
                }
              , package => {
                         type        => 'button'
                       , markup      => "<span color='blue'>package</span>"
                       , icon_name   => 'gtk-save-as'
                       , coderef     => sub { $self->package_template() }
            }
          }
    } );

    $self->pulse( "Creating Gtk3::Ex::DBI::Datasheet - {PARAM}" );

    $self->{param} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                 => $control_db_connection
          , sql                 => {
                                          select        => "template_name , param_name , param_desc , param_default"
                                        , from          => "param"
                                        , order_by      => "param_name"
                                   }
          , auto_incrementing       => 0
          , fields                  => [
            {
                name            => "template_name"
              , renderer        => "hidden"
              , dont_update     => 1
              }
            , {
                name            => "param_name"
              , x_percent       => 30
              }
            , {
                name            => "param_desc"
              , x_percent       => 40
              }
            , {
                name            => "param_default"
              , x_percent       => 30
            }
        ]
            , vbox                    => $self->{builder}->get_object( "param" )
            , recordset_tools_box     => $self->{builder}->get_object( "param_recordset_tools" )
            , before_insert           => sub { $self->before_param_insert }
            , on_insert               => sub { $self->on_param_insert }
        } );

    $self->on_template_current();

    $self->pulse( "Creating Gtk3::Ex::DBI::Form object - {CONFIG}" );
    
    $self->{config} = Gtk3::Ex::DBI::Form->new(
    {
        dbh             => $control_db_connection
      , sql             => {
                              select       => "*"
                            , from         => "config"
                            , order_by     => "sequence_order"
                            , where        => "0=1"
                           }
      , widget_prefix           => "config."
      , builder                 => $self->{builder}
      , status_label            => "record_status"
      , auto_apply              => TRUE
      , on_current              => sub { $self->on_config_current() }
      , on_apply                => sub { $self->on_config_apply() }
      , on_delete               => sub { $self->on_config_delete() }
      , before_delete           => sub { $self->before_config_delete() }
      , on_initial_changed      => sub { $self->on_config_insert() }
      , debug                   => TRUE
      , apeture                 => 1
      , recordset_tools_box     => $self->{builder}->get_object( "config_recordset_tools" )
    } );
    
    $self->pulse( "Creating Gtk3::Ex::DBI::Form - {HARVEST_CONTROL}" );
    
    $self->{harvest_control} = Gtk3::Ex::DBI::Form->new(
    {
        dbh                     => $control_db_connection
      , sql                     => {
                                      select       => "*"
                                    , from         => "harvest_control"
                                    , where        => "0=1"
                                   }
      , widget_prefix           => "harvest_control."
      , builder                 => $self->{builder}
      , on_initial_changed      => sub { $self->on_harvest_control_insert() }
      , debug                   => TRUE
      , apeture                 => 1
      , recordset_tools_box     => $self->{builder}->get_object( "harvest_control_toolsbox" )
    } );
    
    $self->pulse( "Creating Gtk3::Ex::DBI::Datasheet - {CONFIG_PARAMS_LIST}" );
    
    $self->{config_params_list} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh             => $control_db_connection
      , dump_on_error   => 1
      , sql             => {
                              pass_through => "select\n"
                                            . "    param.param_name , case when PV.param_name is null then 0 else 1 end as defined"
                                            . "  , param.param_desc , param.param_default\n"
                                            . "from\n"
                                            . "    param left join\n"
                                            . "    (\n"
                                            . "        select param_name from param_value where processing_group_name = ''\n"
                                            . "    ) PV on param.param_name = PV.PARAM_NAME\n"
                                            . "where\n"
                                            . "    param.template_name = ''\n"
                                            . "order by\n"
                                            . "    param.param_name"
                           }
      , read_only       => TRUE
      , fields          => [
            {
                name          => "param_name"
              , header_markup => "Parameter Name"
              , x_percent     => 100
            }
          , {
                name          => "def"
              , header_markup => "<small>defined</small>"
              , x_absolute    => 50
              , renderer      => "toggle"
            }
          , {
                name          => "param_desc"
              , renderer      => "hidden"
            }
          , {
                name          => "param_default"
              , renderer      => "hidden"
            }
        ]
      , vbox            => $self->{builder}->get_object( "config_params_list" )
      , on_row_select   => sub { $self->on_param_value_select() }
    } );
    
    $self->pulse( "Creating Gtk3::Ex::DBI::Form - {PARAM_VALUE}" );
    
    $self->{param_value} = Gtk3::Ex::DBI::Form->new(
    {
        dbh                     => $control_db_connection,
        sql                     => {
                                    select        => "*"
                                  , from          => "param_value"
                                  , where         => "0=1"
                                }
      , widget_prefix           => "param_value."
      , builder                 => $self->{builder}
      , on_apply                => sub { $self->on_param_value_apply( @_ ) }
      , on_delete               => sub { $self->on_param_value_delete( @_ ) }
      , on_initial_changed      => sub { $self->on_param_value_initial_changed( @_ ) }
      , debug                   => TRUE
      , recordset_tools_box     => $self->{builder}->get_object( "param_value_recordset_tools" )
      , recordset_tool_items    => [ qw ' label undo delete apply strip fullscreen' ]
      , recordset_extra_tools   => {
                                        strip => {
                                            type        => 'button'
                                          , markup      => "<span color='red'>strip whitespace</span>"
                                          , icon_name   => 'gtk-convert'
                                          , coderef     => sub { $self->on_ParamValue_strip_subsequent_clicked() }
                                        }
                                      , fullscreen => {
                                            type        => 'button'
                                          , markup      => "<span color='blue'>fullscreen</span>"
                                          , icon_name   => 'zoom-in'
                                          , coderef     => sub { $self->zoom_param_value() }
                                        }
        }
    } );
    
    $self->on_CONFIG_TEMPLATE_NAME_changed();
    $self->on_config_current();
    
    $self->{processing_group_set_members} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                 => $control_db_connection
      , auto_incrementing   => 0
      , sql                 => {
                                   select           => "*"
                                 , from             => "processing_group_set_members"
                                 , where            => "0=1"
                               }
      , fields              => [
                                   {
                                        name        => "processing_group_set_name"
                                      , renderer    => "hidden"
                                   }
                                 , {
                                        name        => "Processing Group Name"
                                      , x_percent   => 50
                                      , read_only   => 1
                                   }
                               ]
      , vbox                => $self->{builder}->get_object( "processing_group_set_members" )
      , on_row_select       => sub { $self->on_processing_group_set_members( @_ ) }
    } );
    
    $self->{processing_group_sets} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                 => $control_db_connection
      , auto_incrementing   => 0
      , column_sorting      => 1
      , sql                 => {
                                   select           => "*"
                                 , from             => "processing_group_sets"
                                 , order_by         => "processing_group_set_name"
                               }
      , fields              => [
                                   {
                                       name         => "Processing Group Set"
                                     , x_percent    => 100
                                   }
                               ]
      , vbox                => $self->{builder}->get_object( "processing_group_sets" )
      , recordset_tools_box => $self->{builder}->get_object( 'PGS_tools_box' )
      , on_row_select       => sub { $self->on_processing_group_sets_select() }
      , on_apply            => sub { $self->on_processing_group_sets_apply( @_ ) }
    } );
    
    $self->pulse( "Creating Gtk3::Ex::DBI::Datasheet - {PROCESSING_GROUPS}" );

    $self->refresh_repos_combo();

    $self->{processing_group_form} = Gtk3::Ex::DBI::Form->new(
    {
        dbh                     => $control_db_connection
      , sql                     => {
                                      select      => "*"
                                    , from        => "processing_group"
                                    , where       => "processing_group_name = ?"
                                    , bind_values => [ undef ]
                                   }
      , auto_incrementing       => FALSE
      , builder                 => $self->{builder}
      , widget_prefix           => "pgf."
      , debug                   => 1
      , recordset_tools_box     => $self->{builder}->get_object( "pgf_recordset_tools" )
      , recordset_tool_items    => [ "label" , "insert", "undo", "delete", "apply", "package", "rename", "clone" ]
      , recordset_extra_tools   => {
            clone => {
                     type        => 'button'
                   , markup      => "<span color='purple'>clone</span>"
                   , icon_name   => "edit-copy"
                   , coderef     => sub { $self->clone_processing_group() }
            }
          , package => {
                     type        => 'button'
                   , markup      => "<span color='blue'>package</span>"
                   , icon_name   => 'gtk-save-as'
                   , coderef     => sub { $self->package_processing_group() }
            }
          , rename => {
                     type        => 'button'
                   , markup      => "<span color='darkgreen'>rename</span>"
                   , icon_name   => 'gtk-edit'
                   , coderef     => sub { $self->rename_processing_group() }
            }
      }
      , before_delete            => sub { $self->before_processing_group_delete( @_ ) }
      , on_delete                => sub { $self->on_processing_group_delete( @_ ) }
      , on_apply                 => sub { $self->on_processing_group_apply( @_ ) }
    } );

    $self->{processing_groups} = Gtk3::Ex::DBI::Datasheet->new(
    {
        dbh                     => $control_db_connection
      , sql                     => {
                                      select      => "processing_group_name , substr( last_run_timestamp::VARCHAR, 0, 20 ) as last_run_timestamp"
                                                   . " , last_run_seconds , disable_flag , template"
                                    , from        => "processing_group"
                                    , order_by    => "template , processing_group_name"
                                   }
      , read_only               => TRUE
      , auto_incrementing       => FALSE
      , vbox                    => $self->{builder}->get_object( "processing_groups" )
      , before_query            => sub { $self->fetch_max_job_runtime() }
      # , on_apply                => sub { $self->on_processing_group_apply( @_ ) }
      , on_row_select           => sub { $self->on_processing_groups_select() }
      , column_sorting          => TRUE
      , fields                  => [
            {
                name            => "Process Group Name"
              , x_percent       => 100
            }
          , {
                name            => "Last Run"
              , x_absolute      => 150
              , read_only       => TRUE
              , renderer        => "datetime"
            }
          , {
                name            => "Job Seconds"
              , x_absolute      => 100
              , read_only       => TRUE
              , renderer        => "progress"
              , custom_render_functions => [ sub { $self->processing_group_job_seconds_render_function( @_ ) } ]
            }
          , {
                name            => "Disable"
              , x_absolute      => 80
              , renderer        => "toggle"
            }
          , {
                name            => "Template"
              , x_absolute      => 80
              , renderer        => "toggle"
            }
      ]
    } );

    # TODO: push this hack properly into Gtk3::Ex::DBI::Datasheet
    #$self->{processing_groups_filter} = Gtk3::TreeModelFilter->new( $self->{processing_groups}->{treeview}->get_model );
    #$self->{processing_groups_filter}->set_visible_func( sub { $self->processing_groups_filter_filter_function( @_ ) } );
    #$self->{processing_groups}->{treeview}->set_model( $self->{processing_groups_filter} );
    
    # Now we setup the processing group hierarchy treeview 
    my $treeview = $self->{builder}->get_object( 'GroupTreeHierarchy' );
    $treeview->get_selection->set_mode( 'multiple' );
    
    # Sequence column
    my $renderer = Gtk3::CellRendererText->new;
    
    my $column = Gtk3::TreeViewColumn->new_with_attributes(
        "Sequence",
        $renderer,
        'text'  => 0
    );
    
    $column->set_sizing( 'GTK_TREE_VIEW_COLUMN_FIXED' );
    $column->set_fixed_width( 120 );
    $treeview->append_column( $column );

    # Icon column
    $renderer = Gtk3::CellRendererPixbuf->new;

    $column = Gtk3::TreeViewColumn->new_with_attributes(
        ""
      , $renderer
      , 'pixbuf' => 1
    );

    #$column->set_cell_data_func( $renderer, sub { $self->render_pixbuf_cell( @_ ); } );

    $treeview->append_column( $column );

    # Template column
    $renderer = Gtk3::CellRendererText->new;
    
    $column = Gtk3::TreeViewColumn->new_with_attributes(
        "Template"
      , $renderer
      , 'text'  => 2
    );
    
#    $column->set_sizing( 'GTK_TREE_VIEW_COLUMN_FIXED' );
#    $column->set_fixed_width( 400 );
    
    $treeview->append_column( $column );
    
    $treeview->get_selection->signal_connect( changed  => sub { $self->on_hierarchy_select( @_ ); } );
    
    $self->{progress}->set_text( "" );
    $self->{progress}->set_fraction( 0 );

    # $self->update_template_selector;
    
    my $position = $self->{globals}->{config_manager}->simpleGet( 'HierarchyConfigPaned_position' );
    
    if ( $position ) {
        $self->{builder}->get_object( 'HierarchyConfigPaned' )->set_position( $position);
    }
    
    $position = $self->{globals}->{config_manager}->simpleGet( 'TemplateParamsPaned_position' );
    
    if ( $position ) {
        $self->{builder}->get_object( 'TemplateParamsPaned' )->set_position( $position);
    }
    
    $position = $self->{globals}->{config_manager}->simpleGet( 'ConfigParamValuePaned_position' );
    
    if ( $position ) {
        $self->{builder}->get_object( 'ConfigParamValuePaned' )->set_position( $position);
    }
    
    $self->maximize;

    return $self;
    
}

sub on_main_stack_set_focus_child {
    
    my ( $self, $stack ) = @_;
    
    my $visible_child = $stack->get_visible_child_name;
    
    if ( $visible_child eq 'job_configuration' ) {
        
        $self->{job_definition_stack_switcher}->show_all;
        
    } else {
        
        $self->{job_definition_stack_switcher}->hide;
        
    }
    
}

sub processing_groups_filter_filter_function {
    
    my ( $self, $model, $iter, $data ) = @_;
    
    my $pattern_to_match = $self->{builder}->get_object( "ProcessingGroupFilter" )->get_text;
    
    return TRUE if ! $pattern_to_match;
    
    no warnings 'uninitialized';
    
    my $string = $model->get( $iter, $self->{processing_groups}->column_from_column_name( "Process Group Name" ) )
       . " - " . $model->get( $iter, $self->{processing_groups}->column_from_column_name( "Process Group Description" ) );
    
    if ( $string =~ /$pattern_to_match/i ) {
        return TRUE;
    } else {
        return FALSE;
    }
    
}

sub on_ProcessingGroupFilter_changed {
    
    my $self = shift;
    
    $self->{processing_groups_filter}->refilter;
    
}

sub refresh_repos_combo {

    my $self = shift;

    $self->{repos} = $self->{globals}->{config_manager}->all_gui_repositories;

    my $repo_model = Gtk3::ListStore->new( "Glib::String" );

    foreach my $repo ( @{$self->{repos}} ) {
        $repo_model->set(
            $repo_model->append()
          , 0
          , $repo
        );
    }

    my $widget = $self->{builder}->get_object( 'pgf.repository' );

    $widget->set_model( $repo_model );
    $widget->set_entry_text_column( 0 );

}

sub rename_processing_group {
    
    my $self = shift;
    
    my $processing_group_name           = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    if ( ! $processing_group_name ) {
        
        $self->dialog(
            {
                title       => "No processing group selected"
              , type        => "error"
              , text        => "Please select a processing group to rename ..."
            }
        );
        
        return;
        
    }
    
    my $new_name = $self->dialog(
        {
            title       => "Rename a processing group"
          , type        => "input"
          , text        => "Please enter a new processing group name ..."
        }
    );

    if ( ! $new_name ) {
        return;
    }

    my $control_dbh = $self->{config}->{dbh}->connection;
    
    $control_dbh->begin_work
        || die( $control_dbh->errstr );
    
    eval {
        
        $control_dbh->do(
            "update config set processing_group_name = ? where processing_group_name = ?"
          , undef
          , $new_name, $processing_group_name
        ) || die( $control_dbh->errstr );
        
        $control_dbh->do(
            "update harvest_control set processing_group_name = ? where processing_group_name = ?"
          , undef
          , $new_name, $processing_group_name
        ) || die( $control_dbh->errstr );
        
        $control_dbh->do(
            "update param_value set processing_group_name = ? where processing_group_name = ?"
          , undef
          , $new_name, $processing_group_name
        ) || die( $control_dbh->errstr );
        
        $control_dbh->do(
            "update processing_group_set_members set processing_group_name = ? where processing_group_name = ?"
          , undef
          , $new_name, $processing_group_name
        ) || die( $control_dbh->errstr );
        
        # Due to a bug in Gt3::Ex::DBI::Datasheet, we can't update primary keys at the moment
        # ( it needs to store an 'original value' as Gtk3::Ex::DBI::Form does ). So for now,
        # we update the PG record, refresh the datasheet, and select the new row.
        
        $control_dbh->do(
            "update processing_group set processing_group_name = ? where processing_group_name = ?"
          , undef
          , $new_name, $processing_group_name
        ) || die( $control_dbh->errstr );
        
        $self->{processing_groups}->query;
        
        $self->{processing_groups}->select_rows(
            {
                column_no   => $self->{processing_groups}->column_from_sql_name( "processing_group_name" )
              , operator    => "eq"
              , value       => $new_name
            }
        );
        
    };
    
    my $err = $@;
    
    if ( $err ) {
        
        $control_dbh->rollback;
        
        $self->dialog(
            {
                title       => "Error migrating processing group metadata!"
              , type        => "error"
              , text        => $err
            }
        );
        
    } else {
        
        $control_dbh->commit;
        
    }
    
}

sub clone_processing_group {
    
    my ( $self , $new_name ) = @_;
    
    my $processing_group_name           = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    if ( ! $processing_group_name ) {
        
        $self->dialog(
            {
                title       => "No processing group selected"
              , type        => "error"
              , text        => "Please select a processing group to rename ..."
            }
        );
        
        return;
        
    }

    if ( ! $new_name ) {
        $new_name = $self->dialog(
            {
                title       => "Clone Processing Group"
              , type        => "input"
              , text        => "Please enter a new processing group name ..."
            }
        );
    }
    
    my $control_dbh = $self->{config}->{dbh};
    
    # TODO: SQL injection possible
    $control_dbh->do(
        "insert into processing_group\n"
      . "(\n"
      . "    repository\n"
      . "  , processing_group_name\n"
      . "  , processing_group_description\n"
      . "  , custom_args\n"
      . "  , notes\n"
      . "  , disable_flag\n"
      . ") select\n"
      . "    repository\n"
      . "  , ? as processing_group_name \n"
      . "  , processing_group_description\n"
      . "  , custom_args\n"
      . "  , notes\n"
      . "  , disable_flag\n"
      . "from\n"
      . "    processing_group\n"
      . "where\n"
      . "    processing_group_name = ?"
      , [
            $new_name , $processing_group_name
        ]
    );
    
    $control_dbh->do(
        "insert into config\n"
      . "(\n"
      . "    processing_group_name\n"
      . "  , sequence_order\n"
      . "  , parent_sequence_order\n"
      . "  , template_name\n"
      . "  , connection_name\n"
      . "  , target_db_name\n"
      . "  , target_schema_name\n"
      . "  , target_table_name\n"
      . "  , source_db_name\n"
      . "  , source_schema_name\n"
      . "  , source_table_name\n"
      . "  , begin_transaction\n"
      . "  , commit_step\n"
      . "  , disable_flag\n"
      . "  , verified\n"
      . "  , autogenerated\n"
      . "  , notes\n"
      . "  , on_error_continue\n"
      . ") select\n"
      . "    ? as processing_group_name\n"
      . "  , sequence_order\n"
      . "  , parent_sequence_order\n"
      . "  , template_name\n"
      . "  , connection_name\n"
      . "  , target_db_name\n"
      . "  , target_schema_name\n"
      . "  , target_table_name\n"
      . "  , source_db_name\n"
      . "  , source_schema_name\n"
      . "  , source_table_name\n"
      . "  , begin_transaction\n"
      . "  , commit_step\n"
      . "  , disable_flag\n"
      . "  , verified\n"
      . "  , autogenerated\n"
      . "  , notes\n"
      . "  , on_error_continue\n"
      . "from\n"
      . "    config\n"
      . "where\n"
      . "    processing_group_name = ?"
      , [ $new_name, $processing_group_name ]
    );
    
    $control_dbh->do(
        "insert into param_value\n"
      . "(\n"
      . "    processing_group_name\n"
      . "  , sequence_order\n"
      . "  , param_name\n"
      . "  , param_value\n"
      . ")\n select\n"
      . "    ? as processing_group_name\n"
      . "  , sequence_order\n"
      . "  , param_name\n"
      . "  , param_value\n"
      . "from\n"
      . "    param_value\n"
      . "where\n"
      . "    processing_group_name = ?"
      , [
            $new_name
          , $processing_group_name
        ]
    );
    
    $self->{processing_groups}->query;
    
}

sub before_processing_group_delete {
    
    my ( $self ) = @_;

    my $dbh = $self->{processing_groups}->{dbh};
    my $processing_group = $self->{processing_group_form}->get_widget_value( "processing_group_name" );
    $dbh->do( "delete from param_value where processing_group_name = ?", [ $processing_group ] );
    $dbh->do( "delete from config where processing_group_name = ?", [ $processing_group ] );
    
}

sub on_processing_group_delete {

    my ( $self ) = @_;

    $self->{processing_groups}->query();

}

sub on_processing_group_apply {

    my ( $self , $item ) = @_;

    if ( $item->{status} eq 'inserted' ) {
        $self->{processing_groups}->query();
    }

}

sub on_processing_group_sets_apply {
    
    my ( $self, $item ) = @_;
    
    if ( $item->{status} eq 'deleted' ) {
        
        my $dbh = $self->{processing_group_sets}->{dbh};
        
        my $processing_group_set_name = $item->{primary_keys}->{processing_group_set_name};
        
        my $processing_group_members = $dbh->select(
            "select processing_group_name from processing_group_set_members where processing_group_set_name = ?"
          , [ $processing_group_set_name ]
        );
        
        foreach my $processing_group ( @{$processing_group_members} ) {
            $dbh->do( "delete from param_value where processing_group_name = ?", [ $processing_group->{processing_group_name} ] );
            $dbh->do( "delete from config where processing_group_name = ?", [ $processing_group->{processing_group_name} ] );
            $dbh->do( "delete from processing_group where processing_group_name = ?", [ $processing_group->{processing_group_name} ] );
            $dbh->do( "delete from md5_validation_control where src_job like '" . $processing_group->{processing_group_name} . "_-_%'", [] );
        }
        
        $dbh->do( "delete from processing_group_set_members where processing_group_set_name = ?"
          , [ $processing_group_set_name ]
        );
        
        $self->{processing_group_set_members}->query;
        $self->{processing_groups}->query;
        $self->{processing_group_form}->query;
        
    }
    
}

sub on_harvest_control_insert {
    
    my $self = shift;
    
    my $processing_group_name = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    if ( ! $processing_group_name ) {
        
        $self->dialog(
            {
                title       => "Can't insert a HARVEST record yet"
              , type        => "error"
              , text        => "You need to have a PROCESSING GROUP record selected <b><i>before</i></b>"
                             . " inserting a HARVEST record"
            }
        );
        
        return 0;
        
    }
    
    $self->{harvest_control}->set_widget_value( "processing_group_name", $processing_group_name );
    
}

sub on_TestRegex_clicked {
    
    my $self = shift;
    
    my $file_regex      = $self->{harvest_control}->get_widget_value( "file_regex" );
    my $filename        = $self->{harvest_control}->get_widget_value( 'example_filename' );
    
    foreach my $counter ( 1 .. 15 ) {
        $self->{builder}->get_object( 'val_' . $counter )->set_text( '' );
    }
    
    my @matches    = $filename =~ /$file_regex/i;
    
    foreach my $counter ( 1 .. 15 ) {
        $self->{builder}->get_object( 'val_' . $counter )->set_text( '' );
    }
    
    my $counter    = 1;
    
    foreach my $match ( @matches ) {
        $self->{builder}->get_object( 'val_' . $counter )->set_text( $match );
        $counter ++;
    }
    
}

sub on_processing_group_set_members {
    
    my ( $self, $one, $two, $three, $four, $five ) = @_;
    
    my $pg_name = $self->{processing_group_set_members}->get_column_value( "processing_group_name" );
    
    $self->{processing_groups}->select_rows(
        {
            column_no   => $self->{processing_groups}->column_from_sql_name( "processing_group_name" )
          , operator    => "eq"
          , value       => $pg_name
        }
    );


}

sub build_group_treeview {
    
    my $self = shift;
    
    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    my $model = $self->{group_hierarchy_model} = Gtk3::TreeStore->new(
        qw' Glib::Int
            Gtk3::Gdk::Pixbuf
            Glib::String '
    );
    
    my $steps = $self->{globals}->{connections}->{CONTROL}->select(
        "select sequence_order , parent_sequence_order , template_name from config\n"
      . "where processing_group_name = ?\n"
      . "order by sequence_order"
      , [ $processing_group ]
    );
    
    my $sequence_to_iter = {};
    
    foreach my $step ( @{$steps} ) {
        
        # If this step has a parent, we need to get the parent's iter
        my $parent_iter;
        
        if ( $step->{parent_sequence_order} ) {
            $parent_iter = $sequence_to_iter->{ $step->{parent_sequence_order} };
        }
        
        my $this_iter = $model->append( $parent_iter );
        
        $sequence_to_iter->{ $step->{sequence_order} } = $this_iter;

        my $icon_path = $self->get_template_icon_path( $step->{template_name} . ".png" );
        my $icon;

        if ( $icon_path ) {
            $icon = $self->to_pixbuf( $icon_path );
        }

        $model->set(
            $this_iter
          , 0, $step->{sequence_order}
          , 1, $icon
          , 2, $step->{template_name}
        );
        
    }
    
    my $treeview = $self->{builder}->get_object( 'GroupTreeHierarchy' );
    $treeview->set_model( $model );
    $treeview->expand_all;
    
    $self->{group_tree_model} = $model;
    $self->{group_tree_view}  = $treeview;
    
}

sub select_sequence_in_treeview {
    
    my ( $self, $sequence ) = @_;
    
    if ( $self->{group_tree_model} ) {
        
        my $iter = $self->{group_tree_model}->get_iter_first;
        
        while ( $iter ) {
            
            if ( $self->{group_tree_model}->get( $iter, 0 ) == $sequence ) {
                $self->{group_tree_view}->get_selection->select_iter( $iter );
                my $path = $self->{group_tree_model}->get_path( $iter );
                $self->{group_tree_view}->scroll_to_cell( $path, undef, TRUE, 0.5, 0.0 );
                last;
            }
            
            if ( ! $self->{group_tree_model}->iter_next( $iter ) ) {
                last;
            }
            
        }
        
    }
    
}

sub on_hierarchy_select {
    
    my ( $self, $selection ) = @_;
    
    my ( $selected_paths, $model ) = $selection->get_selected_rows;
    
    my $iter;
    
    for my $path ( @{$selected_paths} ) {
        $iter = $model->get_iter( $path );
    }
    
    if ( ! $iter ) {
        return; # shouldn't happen?
    }
    
    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );
    my $sequence_order   = $model->get( $iter, 0 );

    $self->{hierarchy_select_block} = 1;

    $self->{config}->query(
        {
            where       => "processing_group_name = ? and sequence_order = ?"
          , bind_values => [ $processing_group, $sequence_order ]
        }
    );

    $self->{hierarchy_select_block} = 0;

}

sub get_selected_hierarchy_sequences {
    
    my $self = shift;
    
    my $treeview = $self->{builder}->get_object( 'GroupTreeHierarchy' );
    my $selection = $treeview->get_selection;
    
    my ( $selected_paths, $model ) = $selection->get_selected_rows;
    
    my @sequence_orders;
    
    for my $path ( @{$selected_paths} ) {
        push @sequence_orders, $model->get( $model->get_iter( $path ), 0 );
    }
    
    return @sequence_orders;
    
}

sub on_InsertBefore_clicked {

    my $self = shift;

    my @sequence_orders = $self->get_selected_hierarchy_sequences;

    if ( @sequence_orders > 1 ) {
        $self->dialog(
            {
                title   => "Multiple steps selected"
              , type    => "error"
              , text    => "You can insert before 1 step at a time"
            }
        );
        return;
    }

    if ( ! @sequence_orders ) { # nothing selected
        return;
    }

    my $sequence_order = $sequence_orders[0];

    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );

    $self->{globals}->{connections}->{CONTROL}->insert_step_before( $processing_group, $sequence_order );

    $self->build_group_treeview;

    $self->select_sequence_in_treeview( $sequence_order );

}

sub on_InsertAfter_clicked {

    my $self = shift;

    my @sequence_orders = $self->get_selected_hierarchy_sequences;

    if ( @sequence_orders > 1 ) {
        $self->dialog(
            {
                title   => "Multiple steps selected"
              , type    => "error"
              , text    => "You can insert after 1 step at a time"
            }
        );
        return;
    }

    if ( ! @sequence_orders ) { # nothing selected
        return;
    }

    my $sequence_order = $sequence_orders[0];

    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );

    $self->{globals}->{connections}->{CONTROL}->insert_step_after( $processing_group, $sequence_order );

    $self->build_group_treeview;

    $self->select_sequence_in_treeview( $sequence_order + 1 );

}

sub on_MoveUp_clicked {
    
    my $self = shift;
    
    my @sequence_orders = $self->get_selected_hierarchy_sequences;
    
    if ( @sequence_orders > 1 ) {
        $self->dialog(
            {
                title   => "Multiple steps selected"
              , type    => "error"
              , text    => "You can only move 1 step at a time"
            }
        );
        return;
    }
    
    if ( ! @sequence_orders ) { # nothing selected
        return;
    }
    
    my $sequence_order = $sequence_orders[0];
    
    if ( $sequence_order == 1 ) { # already the 1st; can't move up more
        return;
    }
    
    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    $self->{globals}->{connections}->{CONTROL}->move_step_up( $processing_group, $sequence_order );
    
    $self->build_group_treeview;
    
    $self->select_sequence_in_treeview( $sequence_order - 1 );
    
}

sub on_MoveDown_clicked {
    
    my $self = shift;
    
    my @sequence_orders = $self->get_selected_hierarchy_sequences;
    
    if ( @sequence_orders > 1 ) {
        $self->dialog(
            {
                title   => "Multiple steps selected"
              , type    => "error"
              , text    => "You can only move 1 step at a time"
            }
        );
        return;
    }
    
    if ( ! @sequence_orders ) { # nothing selected
        return;
    }
    
    my $sequence_order = $sequence_orders[0];
    
    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    $self->{globals}->{connections}->{CONTROL}->move_step_down( $processing_group, $sequence_order );
    
    $self->build_group_treeview;
    
    $self->select_sequence_in_treeview( $sequence_order + 1 );
    
}

sub on_SetParent_clicked {
    
    my $self = shift;
    
    my @sequence_orders = $self->get_selected_hierarchy_sequences;
    
    if ( @sequence_orders < 2 ) {
        $self->dialog(
            {
                title   => "Do what now?"
              , type    => "error"
              , text    => "To set a parentage, you must select at least 2 steps."
            }
        );
        return;
    }
    
    my $parent_sequence_order = shift @sequence_orders;
    
    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    my $sql = "update config set parent_sequence_order = ?\n"
      . "where processing_group_name = ? and sequence_order in (\n    "
      . join( "\n  , ", @sequence_orders ) . "\n)";
    
    $self->{globals}->{connections}->{CONTROL}->do( $sql, [ $parent_sequence_order, $processing_group ] );
    
    $self->build_group_treeview;
    
}

sub on_UnsetParent_clicked {
    
    my $self = shift;
    
    my @sequence_orders = $self->get_selected_hierarchy_sequences;
    
    if ( ! @sequence_orders ) {
        $self->dialog(
            {
                title   => "Do what now?"
              , type    => "error"
              , text    => "To unset a parentage, you must select the steps whose parentage you want to clear."
            }
        );
        return;
    }
    
    my $processing_group = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    my $sql = "update config set parent_sequence_order = 0\n"
      . "where processing_group_name = ? and sequence_order in (\n    "
      . join( "\n  , ", @sequence_orders ) . "\n)";
    
    $self->{globals}->{connections}->{CONTROL}->do( $sql, [ $processing_group ] );
    
    $self->build_group_treeview;
    
}

sub update_template_selector {
    
    my $self = shift;

    my $templates = $self->{globals}->{connections}->{CONTROL}->select(
        "select template_name from template order by template_name"
    );

    my $template_model = Gtk3::ListStore->new( "Glib::String" , "Gtk3::Gdk::Pixbuf" ); # model for the combo in the template form
    my $config_model   = Gtk3::ListStore->new( "Glib::String" , "Gtk3::Gdk::Pixbuf" ); # model for the combo in the config form

    foreach my $template ( @{$templates} ) {

        my $icon_path = $self->get_template_icon_path( $template->{template_name} . ".png" );
        my $icon;

        if ( $icon_path ) {
            $icon = $self->to_pixbuf( $icon_path );
        }

        $template_model->set(
            $template_model->append
          , 0 , $template->{template_name}
          , 1, $icon
        );

        $config_model->set(
            $config_model->append
          , 0 , $template->{template_name}
          , 1, $icon
        );

    }

    # combo in template form
    my $widget = $self->{builder}->get_object( 'TemplateSelector' );

    $widget->set_model( $template_model );

    if ( ! $self->{template_selector_combo_setup} ) {
        $self->create_combo_renderers( $widget, 0, 1 );
        $self->{template_selector_combo_setup} = 1;
    }

    # combo in config form
    $widget = $self->{builder}->get_object( 'config.template_name' );

    # fetch the current value before replacing the combo ...
    my $current_value;

    # Also note that during construction, we get called *before* the $self->{config} form is created ...
    if ( exists $self->{config} ) {
        $current_value = $self->{config}->get_widget_value( "config.template_name" );
        $self->{config}->{changelock} = TRUE;
    }
    $widget->set_model( $config_model );
    if ( exists $self->{config} ) {
        $self->{config}->set_widget_value( "config.template_name" , $current_value );
        $self->{config}->{changelock} = FALSE;
    }

    if ( ! $self->{config_template_combo_setup} ) {
        $self->create_combo_renderers( $widget, 0, 1 );
        $self->{config_template_combo_setup} = 1;
    }

}

sub on_EditTemplate_clicked {

    my $self = shift;

    my $template_name = $self->{config}->get_widget_value( "template_name" );

    if ( $template_name ) {
        $self->set_widget_value( "TemplateSelector" , $template_name );
        $self->{main_stack}->set_visible_child_name( 'template_configuration' );
    }

}

sub on_TemplateSelector_changed {
    
    my $self = shift;
    
    my $template_name = $self->{config}->get_widget_value( "TemplateSelector" );
    
    $self->{template}->query(
        {
            where       => "template_name = ?"
          , bind_values => [ $template_name ]
        }
    );
    
}

sub on_ChooseTemplateIcon_clicked {

    my $self = shift;

    my $repo = $self->{template}->get_widget_value( "repository" );

    if ( ! $repo ) {
        $self->dialog(
            {
                title      => "Repository?"
              , type       => "error"
              , text       => "Select a repository first"
            }
        );
        return;
    }

    my $template_name = $self->{template}->get_widget_value( "template_name" );

    if ( ! $repo ) {
        $self->dialog(
            {
                title      => "Template Name?"
              , type       => "error"
              , text       => "Enter a template name first"
            }
        );
        return;
    }

    my $path = $self->file_chooser(
        {
            title   => "Please select an icon for this template"
          , action  => "save"
          , type    => "file"
#          , path    => $self->{globals}->{config_manager}->simpleGet( 'window::main:last_template_icon_folder' )
        }
    );

    my $pixbuf = $self->to_pixbuf( $path );

    $pixbuf = $pixbuf->scale_simple( 32 , 32 , 'GDK_INTERP_HYPER' );

    my $target_path = $self->{globals}->{config_manager}->template_icon_path( $repo ) . $template_name . ".png";

    $pixbuf->save( $target_path , "png" );

    $self->update_template_selector;

}

sub on_processing_group_sets_select {
    
    my $self = shift;
    
    my $PGS_NAME = $self->{processing_group_sets}->get_column_value( "processing_group_set_name" );
    
    $self->{processing_group_set_members}->query(
        {
            where       => "processing_group_set_name = ?"
          , bind_values => [ $PGS_NAME ]
        }
    );
    
    my $iter = $self->{processing_group_set_members}->{treeview}->get_model->get_iter_first;

    if ( $iter ) {
        $self->{processing_group_set_members}->{treeview}->get_selection->select_iter( $iter );
    }

}

sub on_PGS_AddMember_clicked {
    
    my $self = shift;
    
    my $processing_group_set_name = $self->{processing_group_sets}->get_column_value( "processing_group_set_name" );
    my $processing_group_name     = $self->{processing_groups}->get_column_value( "processing_group_name" );
    
    if ( ! $processing_group_set_name || ! $processing_group_name ) {
        
        $self->dialog(
            {
                title   => "Process Group data incomplete"
              , type    => "error"
              , text    => "To add a process group to a set, you need to have both items selected ..."
            }
        );
        
        return;
        
    }
    
    $self->{processing_group_set_members}->insert;
    
    $self->{processing_group_set_members}->set_column_value( "processing_group_set_name", $processing_group_set_name );
    $self->{processing_group_set_members}->set_column_value( "processing_group_name", $processing_group_name );
    
    $self->{processing_group_set_members}->apply;
    
}

sub on_PGS_RemoveMember_clicked {
    
    my $self = shift;
    
    $self->{processing_group_set_members}->delete;
    $self->{processing_group_set_members}->apply;
    
}

sub on_CONFIG_TEMPLATE_NAME_changed {
    
    my $self = shift;
    
    if ( ! $self->{config} ) {
        return;
    }
    
    my $template_name = $self->{config}->get_widget_value( "config.template_name" );
    
    my $records = $self->{globals}->{connections}->{CONTROL}->select(
        "select template_text from template where template_name = ?"
      , [ $template_name ]
    );
    
    $self->{config}->set_widget_value( "template_text_read_only", $records->[0]->{template_text} );
    
    # Set up autocompletion for the current template
    
    my $model = $self->{config_params_list}->{treeview}->get_model;
    my $iter = $model->get_iter_first;
    
    # #CONFIG_%# params ...
    my @tokens = qw | #CONFIG_SOURCE_DB_NAME# #CONFIG_TARGET_DB_NAME#
                      #CONFIG_SOURCE_SCHEMA_NAME# #CONFIG_TARGET_SCHEMA_NAME#
                      #CONFIG_SOURCE_TABLE_NAME# #CONFIG_TARGET_TABLE_NAME# |;
    
    while ( $iter ) {
        push @tokens, $model->get( $iter, $self->{config_params_list}->column_from_column_name( "param_name" ) );
        if ( ! $model->iter_next( $iter ) ) {
            last;
        }
    }
    
    foreach my $token ( @{$self->{_complex_tokens}} ) {
        push @tokens, "#$token#";
    }
    
    foreach my $token ( @{$self->{_env_tokens}} ) {
        push @tokens, "#$token#";
    }
    
    my $completion = $self->{builder}->get_object( "param_value.param_value" )->get_completion;
    
    if ( ! $self->{param_value_provider_buffer} ) {
        $self->{param_value_provider_buffer} = Gtk3::SourceView::Buffer->new;
        $self->{param_value_completion_provider} = Gtk3::SourceView::CompletionWords->new( 'params' );
        $self->{param_value_completion_provider}->register( $self->{param_value_provider_buffer} );
        $completion->add_provider( $self->{param_value_completion_provider} );
    }
    
    $self->{param_value_provider_buffer}->set_text( join( " ", @tokens ) );
    
}

sub values_to_autocompletion {
    
    my ( $self, $values ) = @_;
    
    my $liststore = Gtk3::ListStore->new( "Glib::String", "Glib::String" );
        
    my $counter = 0;
    
    foreach my $value ( @{$values} ) {
        $liststore->insert_with_values( $counter, 0, $value, 1, $value );
        $counter ++;
    }
    
    my $entrycompletion = Gtk3::EntryCompletion->new;
    $entrycompletion->set_minimum_key_length( 0 );
    $entrycompletion->set_model( $liststore );
    $entrycompletion->set_text_column( 1 );
    
    $entrycompletion->set_inline_completion( TRUE );
    
    return $entrycompletion;
    
}

sub on_CONFIG_CONNECTION_NAME_changed {
    
    my $self = shift;
    
#    if ( $self->{config} ) {
#        
#        my $connection = $self->{config}->get_widget_value( "CONNECTION_NAME" );
#        
#        if (
#               ( ! $connection ) 
#            || ( $self->{config_connection_name_cache} && $self->{config_connection_name_cache} eq $connection ) 
#        ) {
#            return;
#        }
#        
#        $self->{config_connection_name_cache} = $connection;
#        
#        $self->{config_connection_cache} = Database::Connection::generate(
#            $self->{globals}
#          , $self->{globals}->{config_manager}->get_auth_values( $connection )
#        );
#        
#        my @databases;
#        
#        eval {
#            @databases = $self->{config_connection_cache}->fetch_database_list;
#        };
#        
#        my $completion = $self->values_to_autocompletion( \@databases );
#        $self->{builder}->get_object( "CONFIG.SOURCE_DB_NAME" )->set_completion( $completion );
#        
#        $completion = $self->values_to_autocompletion( \@databases ); # Can't re-use the same completion ...
#        $self->{builder}->get_object( "CONFIG.TARGET_DB_NAME" )->set_completion( $completion );
#        
#    }
    
}

sub on_CONFIG_SOURCE_DB_NAME_focus_out_event {
    
    my $self = shift;
    
#    if ( $self->{config} ) {
#        
#        my $db = $self->{config}->get_widget_value( "SOURCE_DB_NAME" );
#        
#        if (
#            ( ! $db )
#         || ( $self->{config_src_db_cache} && $self->{config_src_db_cache} eq $db )
#        ) {
#            return;
#        }
#        
#        $self->{config_src_db_cache} = $db;
#        
#        my @schemas;
#        
#        $self->{globals}->{suppress_error_dialogs} = 1;
#        
#        eval {
#            @schemas = $self->{config_connection_cache}->fetch_schema_list( $db );
#        };
#        
#        $self->{globals}->{suppress_error_dialogs} = 0;
#        
#        my $completion = $self->values_to_autocompletion( \@schemas );
#        $self->{builder}->get_object( "CONFIG.SOURCE_SCHEMA_NAME" )->set_completion( $completion );
#        
#    }
    
}

sub on_CONFIG_TARGET_DB_NAME_focus_out_event {
    
    my $self = shift;
    
#    if ( $self->{config} ) {
#        
#        my $db = $self->{config}->get_widget_value( "TARGET_DB_NAME" );
#        
#        if (
#               ( ! $db ) 
#            || ( $self->{config_tgt_db_cache} && $self->{config_tgt_db_cache} eq $db ) 
#        ) {
#            return;
#        }
#        
#        $self->{config_tgt_db_cache} = $db;
#        
#        my @schemas;
#        
#        $self->{globals}->{suppress_error_dialogs} = 1;
#        
#        eval {
#            @schemas = $self->{config_connection_cache}->fetch_schema_list( $db );
#        };
#        
#        $self->{globals}->{suppress_error_dialogs} = 0;
#        
#        my $completion = $self->values_to_autocompletion( \@schemas );
#        $self->{builder}->get_object( "CONFIG.TARGET_SCHEMA_NAME" )->set_completion( $completion );
#        
#    }
    
}

sub on_CONFIG_SOURCE_SCHEMA_NAME_focus_out_event {
    
    my $self = shift;
    
#    if ( $self->{config} ) {
#        
#        my $schema = $self->{config}->get_widget_value( "SOURCE_SCHEMA_NAME" );
#        
#        if (
#            ( ! $schema )
#         || ( $self->{config_src_schema_cache} && $self->{config_src_schema_cache} eq $schema )
#        ) {
#            return;
#        }
#        
#        $self->{config_src_schema_cache} = $schema;
#        
#        my $db       = $self->{config}->get_widget_value( "SOURCE_DB_NAME" );
#        my @tables;
#        
#        $self->{globals}->{suppress_error_dialogs} = 1;
#        
#        eval {
#            @tables = $self->{config_connection_cache}->fetch_table_list( $db , $schema );
#        };
#        
#        $self->{globals}->{suppress_error_dialogs} = 0;
#        
#        my $completion = $self->values_to_autocompletion( \@tables );
#        $self->{builder}->get_object( "CONFIG.SOURCE_TABLE_NAME" )->set_completion( $completion );
#        
#    }
    
}

sub on_CONFIG_TARGET_SCHEMA_NAME_focus_out_event {
    
    my $self = shift;
    
#    if ( $self->{config} ) {
#        
#        my $schema = $self->{config}->get_widget_value( "TARGET_SCHEMA_NAME" );
#        
#        if (
#            ( ! $schema )
#         || ( $self->{config_tgt_schema_cache} && $self->{config_tgt_schema_cache} eq $schema )
#        ) {
#            return;
#        }
#        
#        $self->{config_tgt_schema_cache} = $schema;
#        
#        my $db       = $self->{config}->get_widget_value( "TARGET_DB_NAME" );
#        my @tables;
#        
#        $self->{globals}->{suppress_error_dialogs} = 1;
#        
#        eval {
#            @tables = $self->{config_connection_cache}->fetch_table_list( $db, $schema );
#        };
#        
#        $self->{globals}->{suppress_error_dialogs} = 0;
#        
#        my $completion = $self->values_to_autocompletion( \@tables );
#        $self->{builder}->get_object( "CONFIG.TARGET_TABLE_NAME" )->set_completion( $completion );
#        
#    }
    
}

sub on_config_insert {
    
    my $self = shift;
    
    # TODO: shouldn't peek into internals ... expose this via some other method
    if ( $self->{config}->{inserting} ) {
        
        my $processing_group = $self->{processing_group_form}->get_widget_value( "processing_group_name" );
        $self->{config}->set_widget_value( "processing_group_name", $processing_group );
        
        my $max_sequence_order;
        
        my $records = $self->{globals}->{connections}->{CONTROL}->select(
            "select max(sequence_order) as max_sequence_order from config\n"
          . "where processing_group_name = ?"
          , [ $processing_group ]
        );
        
        if ( @{$records} ) {
            $max_sequence_order = $$records[0]->{max_sequence_order};
        }
        
        $self->{config}->set_widget_value( "sequence_order", $max_sequence_order + 1 );
        $self->{config}->set_widget_value( "parent_sequence_order", 0 );
        
    }
    
    return TRUE;
    
}

sub before_config_delete {
    
    my $self = shift;
    
    my $response = $self->dialog(
            {
                type    => "question"
              , title   => "Confirm DELETE?"
              , text    => "Delete CONFIG and associated PARAM_VALUE records?\nYou can't undo this operation. Are you sure?"
            }
    );
    
    if ( $response eq 'yes' ) {
        my $sth = $self->{globals}->{connections}->{CONTROL}->prepare(
            "delete from param_value where processing_group_name = ? and sequence_order = ?"
        );
        $sth->execute( $self->{config}->original_value( "processing_group_name" )
                     , $self->{config}->original_value( "sequence_order" )
        );
        return TRUE;
    } else {
        return FALSE;
    }
    
}

sub on_delete_selected_processing_group_clicked {
    
    my $self = shift;
    
    my $response = $self->dialog(
        {
            type    => "question"
          , title   => "Confirm DELETE?"
          , text    => "Delete <b>ALL CONFIGS</b> and associated <b>PARAM_VALUE</b> records for the processing group you have selected?\n"
                     . "You can't undo this operation. Are you sure?"
        }
    );
    
    if ( $response eq 'no' ) {
        return;
    }
    
    my $processing_group_name = $self->{config_filter_selector}->get_column_value( "processing_group_name" );
    
    my $dbh = $self->{globals}->{connections}->{CONTROL};
    
    my $sth = $dbh->prepare(
        "delete from param_value where processing_group_name = ?"
    ) || return;
    
    $dbh->execute( $sth, [ $processing_group_name ] );
    
    $sth = $dbh->prepare(
        "delete from config where processing_group_name = ?"
    ) || return;
    
    $dbh->execute( $sth, [ $processing_group_name ] );
    
    $self->{config_filter_selector}->query();
    
}

sub on_template_delete {
    
    my $self = shift;

    $self->update_template_selector();
    
}

sub on_template_current {
    
    my $self = shift;
    
    $self->{param}->query(
        {
            where       => "template_name = ?"
          , bind_values => [ $self->{template}->get_widget_value( "template_name" ) ]
        }
    );
    
}

sub on_template_insert {
    
    my $self = shift;
    
    my $repo = $self->gui_repo_dialog;
    
    if ( ! $repo ) {
        return FALSE;
    } else {
        $self->{template}->insert;
        $self->{template}->set_widget_value( "repository", $repo );
        return FALSE;
    }
    
}

sub before_template_apply {
    
    my $self = shift;
    
    # Here we store old / new values of the primary key - TEMPLATE_NAME.
    # As we use text-based PKs, and allow changes,
    # we have to update all FKs if a user changes the PK.
    
    $self->{template_old_key} = $self->{template}->original_value( "template_name" );
    $self->{template_new_key} = $self->{template}->get_widget_value( "template_name" );
    
    if ( $self->{template_old_key} && $self->{template_old_key} ne $self->{template_new_key} ) {
        
        if ( $self->dialog(
                {
                    title   => "Change primary key [template_name]?"
                  , type    => "question"
                  , markup  => "[template_nae] is a primary key used in the tables: ( template , param , config ).\n\n"
                             . "These can all be automatically updated to keep everything in a consistent state, however,\n\n"
                             . "<b><i>it is then up to you to snapshot all affected metadata into a release kit</i></b>.\n\n"
                             . "Are you sure you want to continue?"
                }
             ) eq 'no' 
        ) {
            return 0;
        } else {
            return 1;
        }
        
    } else {
        return 1; # If the PK hasn't changed, allow the update ...
    }
    
    # The rest is done in on_template_apply() ...
    
}

sub on_template_apply {
    
    my $self = shift;
    
    if ( $self->{template_old_key} && $self->{template_old_key} ne $self->{template_new_key} ) {
        
        $self->{globals}->{connections}->{CONTROL}->do(
            "update param set template_name = ? where template_name = ?"
          , [ $self->{template_new_key} , $self->{template_old_key} ]
        );
        
        $self->{globals}->{connections}->{CONTROL}->do(
            "update config set template_name = ? where template_name = ?"
          , [ $self->{template_new_key} , $self->{template_old_key} ]
        );
        
        $self->{template}->query(
            {
                where       => "template_name = ?"
              , bind_values => [ $self->{template_new_key} ]
            }
        );
        
        $self->build_group_treeview;
        
    }
    
    $self->{config}->setup_combo( "template_name" );
    
    $self->update_template_selector;
    
}

sub highlight_template_sql_regex {
    
    # NOT IN USE - replaced with GtkSourceView
    
    my ( $self, $buffer ) = @_;
    
    my @highlight_list = (
        [    '#P\w.*?#'                           , 'param'       ]
      , [    '#E\w.*?#'                           , 'env'         ]
      , [    '#C\w.*?#'                           , 'config'      ]
      , [    '#Q\w.*?#'                           , 'query-param' ]
      , [    '#I\w.*?#'                           , 'iterator'    ]
      , [    '\b[A-Za-z{}_]*\.\.[A-Za-z{}_]*\b'   , 'database'    ] 
    );
    
    my ( $start_iter, $end_iter ) = $buffer->get_bounds;
    my $value = $buffer->get_text( $start_iter, $end_iter, 1 );
    
    for my $regex_highlighter ( @highlight_list ) {
        
        my ( $start_position, $end_position );
        
        my @match_positions = $self->match_all_positions( $$regex_highlighter[0], $value );
        
        foreach my $position_set ( @match_positions ) {
            my $this_start_iter = $buffer->get_iter_at_offset( $$position_set[0] );
            my $this_end_iter   = $buffer->get_iter_at_offset( $$position_set[1] );
            $buffer->apply_tag_by_name( $$regex_highlighter[1], $this_start_iter, $this_end_iter );
        }
        
    }
    
}

sub match_all_positions {
    
    # NOT IN USE - replaced with GtkSourceView
    
    my ( $self, $regex, $string ) = @_;
    
    my @ret;
    
    while ( $string =~ /$regex/mg ) {
        push @ret, [ $-[0], $+[0] ];
    }
    
    return @ret
    
}

sub on_config_current {
    
    my $self = shift;
    
    # When we move to a new config record, we have to fetch the list of associated parameters, and show the ones defined
    
    my $template_name           = $self->{config}->get_widget_value( "template_name" );
    my $processing_group_name   = $self->{config}->get_widget_value( "processing_group_name" );
    my $sequence_order          = $self->{config}->get_widget_value( "sequence_order" );
    
    $self->{config_params_list}->{sql}->{pass_through}
        = "select\n"
        . "    param.param_name, case when PV.param_name is null then 0 else 1 end as defined, param.param_desc, param.param_default\n"
        . "from\n"
        . "    param left join\n"
        . "    (\n"
        . "        select\n"
        . "            param_name\n"
        . "        from\n"
        . "            param_value\n"
        . "        where\n"
        . "            processing_group_name = '" . $processing_group_name . "'\n"
        . "        and sequence_order = " . ( $sequence_order || 0 ) . "\n"
        . "    ) PV on param.param_name = PV.param_name\n"
        . "where\n"
        . "    param.template_name = '" . $template_name . "'\n"
        . "order by\n"
        . "    param.param_name";
    
    print "\n\n" . $self->{config_params_list}->{sql}->{pass_through} . "\n\n";
    
    $self->{config_params_list}->query();
    
    my $model = $self->{config_params_list}->{treeview}->get_model;
    my $iter = $model->get_iter_first;
    
    if ( $iter ) {
        $self->{config_params_list}->{treeview}->get_selection->select_iter( $iter );
    } else {
        $self->{param_value}->query(
            {
                where       => "0=1"
              , bind_values => []
            }
        );
        $self->{builder}->get_object( "ParamDescription" )->set_text( "" );
    }
    
    # moved to on_CONFIG_TEMPLATE_NAME_changed():
    
    #my @params = qw | #CONFIG_SOURCE_DB_NAME# #CONFIG_TARGET_DB_NAME#
    #                  #CONFIG_SOURCE_SCHEMA_NAME# #CONFIG_TARGET_SCHEMA_NAME#
    #                  #CONFIG_SOURCE_TABLE_NAME# #CONFIG_TARGET_TABLE_NAME# |;
    #
    #while ( $iter ) {
    #    push @params, $model->get( $iter, $self->{config_params_list}->column_from_column_name( "PARAM_NAME" ) );
    #    if ( ! $model->iter_next( $iter ) ) {
    #        last;
    #    }
    #}
    #
    #my $completion = $self->{builder}->get_object( "PARAM_VALUE.PARAM_VALUE" )->get_completion;
    #
    #if ( ! $self->{param_value_provider_buffer} ) {
    #    $self->{param_value_provider_buffer} = Gtk3::SourceView::Buffer->new;
    #    $self->{param_value_completion_provider} = Gtk3::SourceView::CompletionWords->new( 'params' );
    #    $self->{param_value_completion_provider}->register( $self->{param_value_provider_buffer} );
    #    $completion->add_provider( $self->{param_value_completion_provider} );
    #}
    #
    #$self->{param_value_provider_buffer}->set_text( join( " ", @params ) );
    
    $self->on_CONFIG_SOURCE_DB_NAME_focus_out_event;
    $self->on_CONFIG_TARGET_DB_NAME_focus_out_event;
    
}

sub on_config_apply {
    
    my $self = shift;

    if ( ! $self->{hierarchy_select_block} ) {

        $self->build_group_treeview;

        my $sequence = $self->{config}->get_widget_value( "sequence_order" );
        $self->select_sequence_in_treeview( $sequence );

    }

}

sub on_config_delete {
    
    my $self = shift;
    
    $self->build_group_treeview;
    
}

sub on_param_value_select {
    
    my $self = shift;
    
    $self->{param_value}->query(
        {
            where       => "processing_group_name = ? and sequence_order = ? and param_name = ?"
          , bind_values => [
                                $self->{config}->get_widget_value( "processing_group_name" )
                              , $self->{config}->get_widget_value( "sequence_order" )
                              , $self->{config_params_list}->get_column_value( "param_name" )
                           ]
        }
    );

    # The description of the parameter
    {
        no warnings 'uninitialized';
        $self->{builder}->get_object( "ParamDescription" )->set_markup(
            "<b>Parameter Value: </b> " . $self->{config_params_list}->get_column_value( "param_desc" )
        );
    }

    # Show the param's default value, and highlight the default value tab selector if there is a default
    my $param_default_value = $self->{config_params_list}->get_column_value( "param_default" );

    if ( defined $param_default_value ) {
        $self->set_widget_value( "ParamDefaultValue" , $param_default_value );
    } else {
        $self->set_widget_value( "ParamDefaultValue" , "" );
    }

    # Show the param value if one exists, and show the default value page if no param value is currently defined
    my $param_value = $self->{param_value}->get_widget_value( "param_value" );

    if ( $param_value ne '' || $param_default_value eq '' ) {
        $self->{builder}->get_object( 'ParamValue_DefaultValueLabel' )->set_text( "Default Value" );
        $self->{builder}->get_object( 'ParamValue_ValueEditorLabel' )->set_markup( "<span color='blue'>Parameter Value Editor</span>" );
        $self->{builder}->get_object( 'ParamValueEntryNotebook' )->set_current_page( 0 );
    } else {
        $self->{builder}->get_object( 'ParamValue_DefaultValueLabel' )->set_markup( "<span color='blue'>Default Value</span>" );
        $self->{builder}->get_object( 'ParamValue_ValueEditorLabel' )->set_text( "Parameter Value Editor" );
        $self->{builder}->get_object( 'ParamValueEntryNotebook' )->set_current_page( 1 );
    }

}

sub on_processing_groups_select {
    
    my $self = shift;
    
    if ( $self->{config_requery_block} ) { 
        print "requery blocked ...\n";
        return 0;
    }
    
    print "filter triggered a requery ...\n";

    $self->{processing_group_form}->query(
        {
            where       => "processing_group_name = ?"
          , bind_values => [ $self->{processing_groups}->get_column_value( "processing_group_name" ) ]
        }
    );

    $self->build_group_treeview;
    
    my $iter = $self->{group_tree_model}->get_iter_first;
    
    if ( $iter ) {
        $self->{group_tree_view}->get_selection->select_iter( $iter );
    } else {
        $self->{config}->query(
            {
                where       => "processing_group_name = ?"
                             . " and sequence_order = ?"
              , bind_values => [
                                    undef
                                  , undef
                               ]
            }
        );
    }
    
    $self->{harvest_control}->query(
        {
            where       => "processing_group_name = ?"
          , bind_values => [ $self->{processing_groups}->get_column_value( "processing_group_name" ) ]
        }
    );
    
}

sub on_param_value_initial_changed {
    
    my $self = shift;
    
    my $param_name = $self->{config_params_list}->get_column_value( "param_name");
    
    if ( ! $param_name ) {
        $self->dialog(
            {
                title   => "Can't insert record yet!",
                type    => "error",
                text    => "You need to select a PARAM from the list before you insert a record here"
            }
        );
        return 0; # This will undo changes in the recordset
    }
    
    my $processing_group_name = $self->{config}->get_widget_value( "processing_group_name" );
    
    if ( ! $processing_group_name ) {
        $self->dialog(
            {
                title   => "Can't insert record yet!",
                type    => "error",
                text    => "You need to have a CONFIG record selected before you can insert a record here"
            }
        );
        return 0; # This will undo changes in the recordset
    }
    
    my $sequence_order = $self->{config}->get_widget_value( "sequence_order" );
    
    if ( ! $sequence_order ) {
        $self->dialog(
            {
                title   => "Can't insert record yet!",
                type    => "error",
                text    => "You need to have a config record selected, AND have a sequence_order value set before you can insert a record here"
            }
        );
        return 0; # This will undo changes in the recordset
    }
    
    $self->{param_value}->set_widget_value( "processing_group_name", $processing_group_name );
    $self->{param_value}->set_widget_value( "sequence_order", $sequence_order );
    $self->{param_value}->set_widget_value( "param_name", $param_name );
    
    $self->{last_edited_param_name} = $param_name;
    
}

sub on_param_value_apply {
    
    my $self = shift;
    
    # requery the config_param_list, which will now display the 'defined' flag if we've just inserted a new record
    
    $self->on_config_current();
    
    $self->{config_params_list}->select_rows(
        {
            column_no   => $self->{config_params_list}->column_from_column_name( "param_name" )
          , operator    => "eq"
          , value       => $self->{last_edited_param_name}
        }
    );
    
}

sub on_param_value_delete {
    
    my $self = shift;
    
    # requery the config_param_list, which will now display the 'defined' flag if we've just inserted a new record
    
    my $param_name = $self->{config_params_list}->get_column_value( "param_name" );
    
    $self->on_config_current();
    
    $self->{config_params_list}->select_rows(
        {
            column_no   => $self->{config_params_list}->column_from_column_name( "param_name" )
          , operator    => "=="
          , value       => $param_name
        }
    );
    
}

sub on_ParamValue_strip_subsequent_clicked {
    
    my $self = shift;
    
    my $param_value = $self->{param_value}->get_widget_value( "param_name" );
    
    my @lines = split "\n", $param_value;
    
    my @new_lines;
    
    foreach my $line ( @lines ) {
        if ( $line =~ /(\s*,*\s*\w*).*/ ) {
            push @new_lines, $1;
        }
    }
    
    my $new_value = join( "\n", @new_lines );
    
    $self->{param_value}->set_widget_value( "param_value", $new_value );
    
}

sub before_param_insert {
    
    my $self = shift;
    
    my $template_name = $self->{template}->get_widget_value( "template_name" );
    
    if ( ! $template_name ) {
        $self->dialog(
            {
                title   => "Can't insert record yet!",
                type    => "error",
                text    => "You need to be navigated to a template record ( on the right )"
            }
        );
        return FALSE;
    } else {
        return TRUE;
    }
    
}

sub on_param_insert {
    
    my $self = shift;
    
    my $template_name = $self->{template}->get_widget_value( "template_name" );
    
    if ( ! $template_name ) {
        $self->dialog(
            {
                title   => "Oh no!",
                type    => "error",
                text    => "Couldn't get the template_name to stamp into this param record. Please investigate ..."
            }
        );
        return FALSE;
    }
    
    $self->{param}->set_column_value( "template_name", $template_name );
    
}

sub on_PopulateParamaters_clicked {
    
    my $self = shift;
    
    my $template_text = $self->{template}->get_widget_value( "template_text" );
    
    my @substitution_parameters = $template_text =~ /#P_[a-zA-Z0-9_]*#/g;
    my %parameters_hash = map { $_ => 0 } @substitution_parameters;
    
    # The rest kinda requires knowledge of Gtk3::Ex::DBI::Datasheet internals ...
    # We're looping through all the elements in the model ( driving the param datasheet )
    # and looking for parameters in the template sql
    # We're also looking for things that are in the model that are NOT in the template sql
    
    my $model = $self->{param}->{treeview}->get_model;
    my $iter = $model->get_iter_first;
    my %model_params;
    
    while ( $iter ) {
        my $param_name = $model->get( $iter, $self->{param}->column_from_column_name( "param_name" ) );
        $model_params{ $param_name } = 0;
        foreach my $param ( keys %parameters_hash ) {
            if ( $param eq $param_name ) {
                # If we find a parameter is defined in the model - mark it now in the hash
                $parameters_hash{ $param } = 1;
                $model_params{ $param } = 1;
            }
            
        }
        if ( ! $model->iter_next( $iter ) ) {
            last;
        }
    }
    
    # Now loop through the parameters again, and insert ones defined in the template that
    # aren't in the param datasheet
    foreach my $param ( keys %parameters_hash ) {
        if ( ! $parameters_hash{ $param } ) {
            if (
                $self->dialog(
                    {
                        title   => "Found a missing parameter"
                      , type    => "question"
                      , text    => "Parameter $param is in the Template SQL, but not registered"
                                 . " in the parameter table. Do you want to insert it?"
                    }
                ) eq 'yes'
            ) {
                $self->{param}->insert;
                $self->{param}->set_column_value( "param_name", $param );
            }
        }
    }
    
    # Finally check for things which we found in the model that weren't in the template sql
    foreach my $param ( keys %model_params ) {
        if ( ! $model_params{ $param } ) {
            if (
                $self->dialog(
                    {
                        title   => "Found an extra parameter"
                      , type    => "question"
                      , text    => "Parameter $param is in the parameter table, but not in the template SQL."
                                 . " Do you want to delete it?"
                    }
                ) eq 'yes'
            ) {
                $self->{param}->select_rows(
                    {
                        column_no   => $self->{param}->column_from_column_name( "param_name" )
                      , operator    => "=="
                      , value       => $param
                    }
                );
                $self->{param}->delete;
            }
        }
    }
    
    $self->dialog(
        {
            title   => "Check complete"
          , type    => "info"
          , text    => "Finished comparing template SQL with parameters"
        }
    );

    return FALSE;

}

sub export_processing_group_type_dml {
    
    my $self = shift;
    
    my $control_db_name         = $self->{globals}->{CONTROL_DB_NAME};
    my $config_manager          = $self->{config}->{dbh}->{config_manager};
    my $processing_group_name   = $self->{processing_groups}->get_column_value( "processing_group_name" );
    my $overlay_path            = $self->overlay_path_dialog || return;
    
    $self->snapshot_metadata(
        {
            dbh                 => $self->{processing_groups}->{dbh}
          , table               => "processing_group"
          , primary_key_array   => [ "processing_group_name" ]
          , keys_aoa            => [ [ $processing_group_name ] ]
          , config_manager      => $config_manager
          , overlay_path        => $overlay_path
          , snapshot_name       => $processing_group_name
        }
    );
    
    $self->snapshot_metadata(
        {
            dbh                 => $self->{processing_groups}->{dbh}
          , table               => "config"
          , primary_key_array   => [ "processing_group_name" ]
          , keys_aoa            => [ [ $processing_group_name ] ]
          , config_manager      => $config_manager
          , overlay_path        => $overlay_path
          , snapshot_name       => $processing_group_name
        }
    );
    
    $self->snapshot_metadata(
        {
            dbh                 => $self->{processing_groups}->{dbh}
          , table               => "param_value"
          , primary_key_array   => [ "processing_group_name" ]
          , keys_aoa            => [ [ $processing_group_name ] ]
          , config_manager      => $config_manager
          , overlay_path        => $overlay_path
          , snapshot_name       => $processing_group_name
        }
    );
    
    $self->snapshot_metadata(
        {
            dbh                 => $self->{processing_groups}->{dbh}
          , table               => "harvest_control"
          , primary_key_array   => [ "processing_group_name" ]
          , keys_aoa            => [ [ $processing_group_name ] ]
          , config_manager      => $config_manager
          , overlay_path        => $overlay_path
          , snapshot_name       => $processing_group_name
        }
    );
    
    $self->dialog(
        {
            title       => "Export Complete"
          , type        => "info"
          , text        => "DMLs and CSV-encoded data for this template + parameters have been exported"
        }
    );
    
}

sub export_template_dml {
    
    my $self                    = shift;
    
    my $template_name           = $self->{template}->get_widget_value( "template_name" );
    my $config_manager          = $self->{config}->{dbh}->{config_manager};
    my $overlay_path            = $self->overlay_path_dialog || return;
    
    $self->snapshot_metadata(
        {
            dbh                 => $self->{config}->{dbh}
          , table               => "template"
          , primary_key_array   => [ "template_name" ]
          , keys_aoa            => [ [ $template_name ] ]
          , config_manager      => $config_manager
          , overlay_path        => $overlay_path
          , snapshot_name       => $self->{template}->get_widget_value( "template_name" )
        }
    );
    
    # We also save PARAM records for this TEMPLATE record
    my $param_names = $self->{param}->{dbh}->select(
        "select param_name from param where template_name = ?"
      , [ $template_name ]
      , "param_name"
    );
    
    if ( keys %{$param_names} ) {
        $self->snapshot_metadata(
            {
                dbh                 => $self->{param_value}->{dbh}
              , table               => "param"
              , primary_key_array   => [ "template_name" ]             # NOTE !!! We delete by TEMPLATE_NAME here
              , keys_aoa            => [ [ $template_name ] ]
              , config_manager      => $config_manager
              , overlay_path        => $overlay_path
              , snapshot_name       => $self->{template}->get_widget_value( "template_name" )
            }
        );
    }
    
    $self->dialog(
        {
            title       => "Export Complete"
          , type        => "info"
          , text        => "DMLs and CSV-encoded data for this template + parameters have been exported"
        }
    );
    
}

sub gui_repo_dialog {
    
    my $self = shift;
    
    my $gui_repos = $self->{globals}->{config_manager}->all_gui_repositories;
    
    my $repo = $self->dialog(
        {
            title               => "Select a repository ..."
          , type                => "options"
          , markup              => "Templates and Jobs are stored in a repository that <i>can't</i> change"
          , options             => $gui_repos
        }
    );
    
    return $repo;
    
}

sub overlay_path_dialog {
    
    my $self = shift;
    
    my $overlays        = $self->{globals}->{config_manager}->gui_overlay_names_and_types;
    
    my @dialog_options;
    
    foreach my $overlay ( @{$overlays} ) {
        push @dialog_options, $overlay->{OverlayName} . ":\n" . $overlay->{OverlayPath};
    };
    
    push @dialog_options, "core:\n" . $self->{globals}->{paths}->{app};
    
    my $schema_type = $self->dialog(
        {
            title               => "Select a schema type to export ..."
          , type                => "options"
          , text                => "The schema type determines the location where objects will be saved"
          , options             => \@dialog_options
        }
    );
    
    my ( $overlay_type, $overlay_path );
    
    if ( ! $schema_type ) {
        
        return;
        
    } else {
        
        if ( $schema_type =~ /(.*):\n(.*)/ ) {
            ( $overlay_type, $overlay_path ) = ( $1, $2 );
        } else {
            $overlay_path = $schema_type;
        }
        
    }
    
    return $overlay_path;
    
}

sub snapshot_metadata {
    
    my ( $self, $options ) = @_;
    
    # $options:
    #{
    #    dbh                 => $dbh
    #  , table               => $table
    #  , primary_key_array   => $primary_key_array
    #  , keys_aoa            => $keys_aoa
    #  , config_manager      => $config_manager
    #  , overlay_path        => $overlay_path
    #  , snapshot_name       => $snapshot_name
    #}
    
    my @created = make_path(
        $options->{config_manager}->metadata_path( $options->{overlay_path} )
      , {
            verbose     => 1
        }
    );
    
    my $file_path;
    
    my $sql = "select * from $options->{table}";
    
    # We build a filter string, based on the primary key array, and the AoA of keys. Note that we *don't* use placeholders
    # here, because the filter we're assembling will also be used to do the 'scope delete' for the DML. We DO escape the values ...
    
    my ( @all_filters_array, $all_filters_string );
    
    foreach my $keys_array ( @{$options->{keys_aoa}} ) {
        
        my @this_filter;
        
        foreach my $key_position ( 0 .. @{$options->{primary_key_array}} - 1 ) {
            push @this_filter, $options->{primary_key_array}[$key_position] . " = '" . $self->escape_value( $$keys_array[$key_position] ) . "'";
        }
        
        push @all_filters_array, "( " . join( " and ", @this_filter ) . " )";
        
    }
    
    $all_filters_string = join( "\n or ", @all_filters_array );
    
    if ( $all_filters_string ) {
        
        # If we're assembled filters, we want to generate a 'delete from scope' type DML
        
        $sql .= "\nwhere\n$all_filters_string";
        
        $file_path = $options->{config_manager}->metadata_path( $options->{overlay_path} )
                  . "/" . $options->{config_manager}->get_next_control_sequence( $options->{overlay_path} )
                  . "_" . lc( $options->{table} ) . "_-_" . lc( $options->{snapshot_name} ) . ".dml";
        
        eval {
            
            open EXPORT_FILE, ">>" . $file_path or die( $! );
            select EXPORT_FILE;
            $| = 1; # turn off output buffering
            select STDOUT;
            
            print EXPORT_FILE "delete from $options->{table} where $all_filters_string;\n";
            
            close EXPORT_FILE or die( $! );
            
        };
        
        my $err = $@;
        
        if ( $err ) {
            
            $self->dialog(
                {
                    title       => "Failed to write DML"
                  , type        => "error"
                  , text        => $err
                }
            );
            
            return;
            
        }        
        
    }
    
    $file_path = $options->{config_manager}->metadata_path( $options->{overlay_path} )
         . "/" . $options->{config_manager}->get_next_control_sequence( $options->{overlay_path} )
         . "_" . lc( $options->{table} ) . "_-_" . lc( $options->{snapshot_name} ) . ".csv";
    
    $options->{dbh}->sql_to_csv(
        {
            file_path       => $file_path
          , delimiter       => ","
          , quote_char      => '"'
          , encoding        => "utf8"
          , column_headers  => 1
          , sql             => $sql
          , dont_force_case => 1
        }
    );
    
}

sub template_copy {

    my $self = shift;

    my $template_name = $self->{template}->get_widget_value( "template_name" );

    my $new_template_name = $self->dialog(
        {
            title       => "New template name"
          , type        => "input"
          , default     => $template_name . "_CLONE"
        }
    );

    if ( ! $new_template_name ) {
        return;
    }

    $self->{globals}->{connections}->{CONTROL}->do(
        "insert into template\n"
      . "(\n"
      . "    template_name\n"
      . "  , template_desc\n"
      . "  , template_text\n"
      . "  , class\n"
      . "  , repository\n"
      . ") select\n"
      . "    ?\n"
      . "  , template_desc\n"
      . "  , template_text\n"
      . "  , class\n"
      . "  , repository\n"
      . "from\n"
      . "    template\n"
      . "where\n"
      . "    template_name = ?"
      , [ $new_template_name , $template_name ]
    );

    $self->{globals}->{connections}->{CONTROL}->do(
        "insert into param\n"
      . "(\n"
      . "    template_name\n"
      . "  , param_name\n"
      . "  , param_desc\n"
      . "  , param_default\n"
      . ") select\n"
      . "    ?\n"
      . "  , param_name\n"
      . "  , param_desc\n"
      . "  , param_default\n"
      . "from\n"
      . "    param\n"
      . "where\n"
      . "    template_name = ?"
      , [ $new_template_name , $template_name]
    );

    $self->update_template_selector;

}

sub package_template {

    my $self = shift;

    my $template_name = $self->{template}->get_widget_value( "template_name" );
    my $repository    = $self->{template}->get_widget_value( "repository" );

    if ( ! $repository ) {
        $self->dialog(
            {
                title   => "No repository!"
              , type    => "error"
              , text    => "Can't create a snapshot without a repository!"
            }
        );
        return;
    }

    my $template = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from template where template_name = ?"
      , [ $template_name ]
    );

    my $parameters = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from param where template_name = ? order by param_name"
      , [ $template_name ]
    );

    my $template_package_def = to_json(
        {
            template => {
                pre    => [ "delete from template where template_name = '$template_name'" ]
              , data   => $template
            }
          , param => {
                pre    => [ "delete from param where template_name = '$template_name'" ]
              , data   => $parameters
            }
        }
      , { pretty => 1 }
    );

    $self->create_package(
        $template_package_def
      , 'template'
      , $template_name
      , $repository
    );

}

sub package_processing_group {

    my $self = shift;

    my $processing_group_name = $self->{processing_groups}->get_column_value( "processing_group_name" );
    my $repository            = $self->{processing_group_form}->get_widget_value( "repository" );

    if ( ! $repository ) {
        $self->dialog(
            {
                title   => "No repository!"
              , type    => "error"
              , text    => "Can't create a snapshot without a repository!"
            }
        );
        return;
    }

    my $processing_group = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from processing_group where processing_group_name = ?"
      , [ $processing_group_name ]
    );

    # We don't want to include these in the package ...
    delete $processing_group->[0]->{LAST_RUN_SECONDS};
    delete $processing_group->[0]->{LAST_RUN_TIMESTAMP};

    my $configs = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from config where processing_group_name = ? order by sequence_order"
      , [ $processing_group_name ]
    );

    my $param_values = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from param_value where processing_group_name = ? order by sequence_order"
      , [ $processing_group_name ]
    );

    my $harvest = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from harvest_control where processing_group_name = ?"
      , [ $processing_group_name ]
    );

    my $job_json = to_json(
        {
            processing_group => {
                pre         => [ "delete from processing_group where processing_group_name = '$processing_group_name'" ]
              , data        => $processing_group
            }
          , config          => {
                pre         => [ "delete from config where processing_group_name = '$processing_group_name'" ]
              , data        => $configs
            }
          , param_value     => {
                pre         => [ "delete from param_value where processing_group_name = '$processing_group_name'" ]
              , data        => $param_values
            }
          , harvest_control => {
                pre         => [ "delete from harvest_control where processing_group_name = '$processing_group_name'" ]
              , data        => $harvest
           }
        }
      , { pretty => 1 }
    );

    $self->create_package(
        $job_json
      , 'job'
      , $processing_group_name
      , $repository
    );

}

sub create_package {
    
    my ( $self , $json_object, $type, $object_name, $repository ) = @_;

    my $releases_aoh  = $self->{globals}->{connections}->{CONTROL}->select(
        "select release_name from releases where release_open = 1 and repository = ?"
      , [ $repository ]
    );
    
    my $releases;
    
    foreach my $release ( @{$releases_aoh} ) {
        push @{$releases}, $release->{release_name};
    }

    my $release_name;

    if ( $releases ) {

        $release_name = $self->dialog(
            {
                title       => "Select a release to add package to"
              , type        => "options"
              , text        => "You can OPTIONALLY add this package to an open release. Click the Cancel button to skip this part"
              , options     => $releases
              , orientation => 'vertical'
            }
        );

    } else {

        my $response = $self->dialog(
            {
                title       => "No open release"
              , type        => "question"
              , text        => "There are no open releases for the repository [$repository]. You can continue ( ie dump the object to the filesystem )"
                             . " if you want to, but it won't be visible to the package manager. As a rule, you should always allocate packages to a release.\n\n"
                             . "Do you want to continue?"
            }
        );

        if ( $response ne 'yes' ) {
            return;
        }

    }

    my $timestamp = $self->timestamp;
    my $user = $self->{globals}->{self}->{user_profile};
    
    my $snapshot_name = $object_name . "." . $timestamp . "." . $user . ".json";

    my $snapshot_dir  = $self->{globals}->{config_manager}->repo_path(
        'gui'
      , $repository
      , ( $self->{globals}->{self}->{flatpak} ? 'persisted' : 'builtin' )
    ) . "$type/$object_name/";

    eval {

        if ( ! -d $snapshot_dir ) {
            print "Target directory [$snapshot_dir] doesn't exist ... creating ...\n";
            my @created = make_path(
                $snapshot_dir
              , {
                    verbose     => 1
                }
            );
            print "Created:\n" . to_json( \@created , { pretty => 1 } );
        }

        my $full_snapshot_path = $snapshot_dir . $snapshot_name;

        print "Full path to package: $full_snapshot_path\n";

        open FH, ">" . $full_snapshot_path or die( "Can't open file:\n" . $! );
        print FH $json_object;
        close FH or die( "Error writing to file:\n" . $! );
    };
    
    my $err = $@;
    
    if ( $err ) {
        $self->dialog(
            {
                title       => "Couldn't create package"
              , type        => "error"
              , text        => $err
            }
        );
        return;
    }

    if ( $release_name && $release_name ne '' ) {

        # Delete any older instances of this template from the selected release
        $self->{globals}->{connections}->{CONTROL}->do(
            "delete from release_packages\n"
          . "where package_type = ? and release_name = ? and object_name = ?"
          , [ $type , $release_name , $object_name ]
        );

        # Add this snapshot to the release
        $self->{globals}->{connections}->{CONTROL}->do(
            "insert into release_packages ( package_type , release_name , object_name , release_package , object_changed ) values ( ? , ? , ? , ? , 1 ) "
          , [ $type , $release_name , $object_name , $snapshot_name ]
        );

    }

    $self->dialog(
        {
            title       => "Package created"
          , type        => "info"
          , text        => "A package of the [$object_name] object has been created"
        }
    );
    
}

sub escape_value {
    
    my $self = shift;
    my $value = shift;
    
    $value =~ s/\'/\'\'/g;
    
    return $value;
    
}

sub on_AnotherLaunchGroup_clicked {
    
    my $self = shift;
    
    my $launcher    = $self->open_window( 'window::framework_launcher', $self->{globals} );
    
    my $custom_args = $self->{processing_group_form}->get_widget_value( "custom_args" );
    
    if ( $custom_args  ) {
        $launcher->set_widget_value( "CustomArgs", $custom_args );
    } else {
        my $pg_name = $self->{processing_groups}->get_column_value( "processing_group_name" );
        $launcher->{builder}->get_object( "ProcessingGroupName" )->set_text( $pg_name );
    }
    
}

sub flash_progress_bar {
    
    # This code flashes labels and things based on flags that get set when checks are run
    
    my ( $self, $mode )  = @_;
    
    if ( $mode ) {
        
        # We're in the 'on' state ( non-standard colour ). Reset colours back to the 'standard' state
        
        my $text = $self->{status_lbl}->get_text;
        $self->{status_lbl}->set_markup( "<span color='red'>$text</span>" );
        
        # Queue the next action: flash OFF
        Glib::Timeout->add( FLASH_TIMEOUT, sub { $self->flash_progress_bar( FALSE ) } );
        
    } else {
        
        my $text = $self->{status_lbl}->get_text;
        $self->{status_lbl}->set_text( $text );
        
        # Queue the next action: flash ON
        Glib::Timeout->add( FLASH_TIMEOUT, sub { $self->flash_progress_bar( TRUE ) } );
        
    }
    
    return FALSE;
    
}

sub autogen {
    
    my ( $self, $definition ) = @_;
    
    # This method applies values from the definitions we've been passed to the
    # config and param_value objects
    
    $self->{main_stack}->set_visible_child_name( 'job_configuration' );
    
    $self->kick_gtk;
    
    if ( exists $definition->{group} ) {
        
        my %group = %{$definition->{group}};
        
        $self->{sequence_order} = 0;
        
        $self->{processing_groups}->query();
        
        $self->kick_gtk;
        
        my $rows = $self->{processing_groups}->select_rows(
            {
                column_no   => $self->{processing_groups}->column_from_column_name( "Process Group Name" )
              , operator    => 'eq'
              , value       => $group{PROCESSING_GROUP_NAME}
            }
        );
        
        $self->kick_gtk;
        
        if ( ! $rows ) {
            $self->{processing_groups}->insert();
            foreach my $column_name ( keys %group ) {
                $self->{processing_groups}->set_column_value( $column_name, $group{$column_name} );
            }
            $self->{processing_groups}->apply;
        }
        
    }
    
    $self->{builder}->get_object( 'job_definition_stack' )->set_visible_child_name( 'configs_and_params' );
    
    $self->kick_gtk;
    
    $self->{sequence_order} ++;
    
    # Query the config object
    $self->{config}->query(
        {
            where       => "processing_group_name = ?"
                         . " and sequence_order = ?"
          , bind_values => [
                                $definition->{config}->{processing_group_name}
                              , $self->{sequence_order}
                           ]
        }
    );
    
    $self->kick_gtk;

    # First, test to see that the passed template exists. If it doesn't, we'll have some nasty errors ...
    my $template_name = $definition->{config}->{template_name};

    my $model = $self->{config}->get_object( 'template_name' )->get_model;
    my $iter  = $model->get_iter_first;
    my $found = 0;

    while ( $iter ) {
        my $value = $model->get( $iter , 0 );
        if ( $value eq $template_name ) {
            $found = 1;
            last;
        }
        if ( ! $model->iter_next( $iter ) ) {
            last;
        }
    }

    if ( ! $found ) {
        $self->dialog(
            {
                title    => "Template not found"
              , type     => "error"
              , text     => "Template [$template_name] not found! Skipping this ETL step ..."
            }
        );
        return;
    }
    
    # Set a value for each field give
    foreach my $field ( keys %{$definition->{config}} ) {
        $self->{config}->set_widget_value( $field, $definition->{config}->{$field} );
    }
    
    $self->{config}->set_widget_value( "sequence_order", $self->{sequence_order} );
    $self->{config}->set_widget_value( "autogenerated", 1 );
    
    # Apply changes
    $self->{config}->apply();
    
    $self->kick_gtk;
    
    # Now the param_value records ...
    foreach my $param ( keys %{$definition->{param_value}} ) {
        
        my $value = $definition->{param_value}->{$param};
        
        # Query the param_value object
        $self->{config_params_list}->select_rows(
            {
                column_no   => $self->{config_params_list}->column_from_sql_name( "param_name" )
              , operator    => "eq"
              , value       => $param
            }
        );
        
        $self->kick_gtk;
        $self->on_param_value_select;
        $self->kick_gtk;
        
        # Set / delete the value
        if ( defined $value ) {
            $self->{param_value}->set_widget_value( "param_value", $value );
            $self->{param_value}->apply();
        } else {
            $self->{param_value}->delete;
        }
        
        $self->kick_gtk;
        
    }
    
    $self->kick_gtk;
    
}

sub on_HierarchyConfigPaned_notify {
    
    my ( $self, $widget, $something, $something_else ) = @_;
    
    my $position = $widget->get_position;
    
    if ( $position ) {
        $self->{globals}->{config_manager}->simpleSet( 'HierarchyConfigPaned_position', $position );
    }
    
}

sub on_TemplateParamsPaned_notify {
    
    my ( $self, $widget, $something, $something_else ) = @_;
    
    my $position = $widget->get_position;
    
    if ( $position ) {
        $self->{globals}->{config_manager}->simpleSet( 'TemplateParamsPaned_position', $position );
    }
    
}

sub on_ConfigParamValuePaned_notify {
    
    my ( $self, $widget, $something, $something_else ) = @_;
    
    my $position = $widget->get_position;
    
    if ( $position ) {
        $self->{globals}->{config_manager}->simpleSet( 'ConfigParamValuePaned_position', $position );
    }
    
}

sub on_ParamValueStepUndo_clicked {
    
    my $self = shift;
    
    $self->{builder}->get_object( "PARAM_VALUE.PARAM_VALUE" )->get_buffer->undo;
    
}

sub on_ParamValueStepRedo_clicked {
    
    my $self = shift;
    
    $self->{builder}->get_object( "PARAM_VALUE.PARAM_VALUE" )->get_buffer->redo;
    
}

sub on_TemplateUndo_clicked {
    
    my $self = shift;
    
    $self->{builder}->get_object( "TEMPLATE.TEMPLATE_TEXT" )->get_buffer->undo;
    
}

sub on_TemplateRedo_clicked {
    
    my $self = shift;
    
    $self->{builder}->get_object( "TEMPLATE.TEMPLATE_TEXT" )->get_buffer->redo;
    
}

sub fetch_max_job_runtime {
    
    my $self = shift;
    
    $self->{max_runtimes} = $self->{globals}->{connections}->{LOG}->select(
        "select   batch_identifier , max(processing_time) as max_runtime\n"
      . "from     batch_ctl\n"
      . "group by batch_identifier"
      , undef
      , "batch_identifier"
    );
    
}

sub processing_group_job_seconds_render_function {
    
    my ( $self , $column, $cell, $model, $iter ) = @_;
    
    my $pg_name = $model->get( $iter, $self->{processing_groups}->column_from_column_name( "Process Group Name" ) );
    my $seconds = $model->get( $iter, $self->{processing_groups}->column_from_column_name( "Job Seconds" ) );
    
    my $this_render_value = $seconds / ( $self->{max_runtimes}->{$pg_name}->{max_runtime} || 1 ) * 100;
    
    #my $gvalue = Glib::Object::Introspection::GValueWrapper->new( 'Glib::Int', $this_render_value );
    #$cell->set( value => $gvalue );
    
    $cell->set( value => $this_render_value );
    $cell->set( text  => ( $seconds || "" ) );
    
}

sub zoom_param_value {
    
    my $self = shift;
    
    if ( ! $self->{param_value_zoom_state} ) {
        
        $self->{ConfigParamValuePaned_normal_state} = $self->{builder}->get_object( 'ConfigParamValuePaned' )->get_position;
        $self->{builder}->get_object( 'ConfigParamValuePaned' )->set_position( 0 );
        
        $self->{TemplateParamsPaned_normal_state}   = $self->{builder}->get_object( 'TemplateParamsPaned' )->get_position;
        $self->{builder}->get_object( 'TemplateParamsPaned' )->set_position( 0 );
        
        $self->{param_value_zoom_state} = 1;
        
    } else {
        
        $self->{builder}->get_object( 'ConfigParamValuePaned' )->set_position( $self->{ConfigParamValuePaned_normal_state} );
        $self->{builder}->get_object( 'TemplateParamsPaned' )->set_position( $self->{TemplateParamsPaned_normal_state} );
        
        $self->{param_value_zoom_state} = 0;
        
    }
    
}

sub on_any_template_text_query_tooltip {
    
    my ( $self, $sourceview, $window_x, $window_y, $keyboard_mode, $tooltip ) = @_;
    
    my ( $buffer_x, $buffer_y ) = $sourceview->window_to_buffer_coords( 'GTK_TEXT_WINDOW_TEXT', $window_x, $window_y );
    
    my ( $trailing_1, $iter_1 ) = $sourceview->get_iter_at_location( $buffer_x, $buffer_y );
    my ( $trailing_2, $iter_2 ) = $sourceview->get_iter_at_location( $buffer_x, $buffer_y );
    
    if ( ! $iter_1->inside_word ) {
        return FALSE;
    }
    
    my $buffer = $sourceview->get_buffer;
    
    # Determine whether there's a # somewhere to the left of the
    # current position before the next space
    
    my $current_char = '';
    my $offset = $iter_1->get_offset;
    
    my $iter_hit_end;
    
    while ( $current_char ne ' ' && $current_char ne '#' && ! $iter_hit_end ) {
        
        $offset -- if $offset > 0;
        
        $current_char = $buffer->get_text(
                                            $buffer->get_iter_at_offset( $offset )
                                          , $buffer->get_iter_at_offset( $offset + 1 )
                                          , TRUE
                                         );
        
        if ( $offset == 0 ) {
            $iter_hit_end = 1;
        }
        
    }
    
    if ( $current_char eq ' ' || ( $iter_hit_end && ! $current_char eq '#' ) ) {
        # We encountered a space before a # ... OR we've backtracked to the beginning before finding a #. Bail.
        return FALSE;   
    }
    
    $iter_hit_end = 0;
    $iter_1 = $buffer->get_iter_at_offset( $offset );
    
    # Determine whether there's a # somewhere to the right of the
    # current position before the next space
    
    $current_char = '';
    $offset = $iter_2->get_offset;
    
    while ( $current_char ne ' ' && $current_char ne '#' && ! $iter_hit_end ) {
        
        $current_char = $buffer->get_text(
                                            $buffer->get_iter_at_offset( $offset )
                                          , $buffer->get_iter_at_offset( $offset + 1 )
                                          , TRUE
                                         );
        
        $offset ++;
        
        if ( $buffer->get_iter_at_offset( $offset )->is_end ) {
            $iter_hit_end = 1;
        }
        
    }
    
    if ( $current_char eq ' ' || ( $iter_hit_end && ! $current_char eq '#' ) ) {
        # We encountered a space before a # ... OR we've stepped forwards to the end before finding a #. Bail.
        return FALSE;   
    }
    
    $iter_2 = $buffer->get_iter_at_offset( $offset );
    
    # If we've gotten to this point, we've got a token ...
    my $token = $buffer->get_text( $iter_1, $iter_2, TRUE );
    
    my $token_docs;
    
    if ( $token =~ /#(ENV_(.*))#/ ) {
        
        my ( $token , $type ) = ( $1 , $2 );
        
        if ( exists tokens::ENV_TOKENS->{$token} ) {
            $token_docs = tokens::ENV_TOKENS->{$token};
        } else {
            warn "Didn't find token ENV [$token] in token doc\n";
        }
        
    } elsif ( $token =~ /#(COMPLEX_(.*))#/ ) {
        
        my ( $token , $type ) = ( $1 , $2 );
        
        if ( exists tokens::COMPLEX_TOKENS->{$token} ) {
            $token_docs = tokens::COMPLEX_TOKENS->{$token};
        } else {
            warn "Didn't find token COMPLEX [$token] in token doc\n";
        }
        
    } elsif ( $token =~ /#I_(\w*)\.(\w*)/ ) {
        
        my ( $iterator_name , $iterator_field ) = ( $1 , $2 );
        
        $token_docs = {
            short_desc      => "Iterator [$iterator_name] , Field [$iterator_field]"
          , long_desc       => "Iterators are like recordsets. In the job sequence editor, arrange a step or set of steps"
                             . " 'inside' another step, and all these 'inside' steps will be executed once per record inside the iterator"
                             . "\n\n"
                             . "This token will return the current value of the field [$iterator_field] inside the iterator named [$iterator_name]"
        };
        
    } elsif ( $token =~ /#P_(\w*)/ ) {
        
        my $param_name = $1;
        
        $token_docs = {
            short_desc      => "User parameter: [$param_name]"
          , long_desc       => "Parameters are key/value pairs that are registered against a template."
                             . "\n\n"
                             . "This token will return the value for user parameter: [$param_name]"
        }
        
    } elsif ( $token =~ /#Q_(\w*)/ ) {
        
        my $param_name = $1;
        
        $token_docs = {
            short_desc      => "Query parameter: [$param_name]"
          , long_desc       => "Query Parameters are key/value pairs that are populated by the execution of a template that uses the class SQL::Q_Params ( eg the SQL_Q_PARAMS template )."
                             . "\n\n"
                             . "This token will return the value for query parameter: [$param_name]"
        }
        
    } elsif ( $token =~ /#J_(\w*)/ ) {
        
        my $json_param = $1;
        
        $token_docs = {
            short_desc      => "JSON parameter: [$json_param]"
          , long_desc       => "Smart Frameworks can accept additional args on the command-line:\n"
                             . " --args={}\n"
                             . "  ... where you place your JSON-encoded args ARRAY ( key/value pairs ) between the curly brackets"
                             . "\n\n"
                             . "This token will return the value for the command-line arg: [$json_param]"
        }
    }
    
    if ( $token_docs) {
        
        $tooltip->set_markup( "<b>" . $token_docs->{short_desc} . "</b>\n\n" . $token_docs->{long_desc} );
        return TRUE;
        
    } else {
        
        return FALSE;
        
    }
    
    return TRUE;
    
}

sub on_Resize_clicked {
    
    my $self = shift;
    
    $self->resize_dialog();
    
}

sub on_main_destroy {
    
    my $self = shift;
    
    for my $item ( qw /
        config
        config_params_list
        config_filter_selector
        param_value
        template
        param
        processing_groups
    / ) {
        if ( exists $self->{ $item } ) {
            $self->{ $item }->destroy();
            $self->{ $item } = undef;
        }
    }
    
    $self->close_window();
    
}

1;
