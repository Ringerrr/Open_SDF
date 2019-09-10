package window::release_manager;

use warnings;
use strict;

use 5.20.0;

use parent 'window';

use Glib qw( TRUE FALSE );

use File::Basename;
use File::Copy;
use File::Temp qw/ tempdir /;

use JSON;

sub new {
    
    my ( $class, $globals, $options ) = @_;
    
    my $self;
    
    $self->{globals} = $globals;
    $self->{options} = $options;
    
    bless $self, $class;
    
    $self->{builder} = Gtk3::Builder->new;
    
    $self->{builder}->add_objects_from_file(
        $self->{options}->{builder_path}
      , "release_manager"
    );
    
    $self->{builder}->connect_signals( undef, $self );

    if ( $options->{close_after_update_check} ) {
        $self->hide();
    }

    $self->{builder}->get_object( "release_manager" )->maximize;
    
    $self->{repos} = $self->{globals}->{config_manager}->all_gui_repositories;

    $self->{releases} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                     => $self->{globals}->{config_manager}->sdf_connection( "CONTROL" )
          , column_sorting          => 1
          , sql                     => {
                                            select      => "REPOSITORY, RELEASE_NAME, RELEASE_OPEN, RELEASE_APPLIED"
                                          , from        => "releases"
                                          , order_by    => "release_name"
                                       }
          , primary_keys            => [ "RELEASE_NAME" ]
          , force_upper_case_fields => 1
          , auto_incrementing       => 0
          , multi_select            => 1
          , fields                  => [
                                        {
                                            name        => "Repository"
                                          , x_percent   => 30
                                          , read_only   => 1
                                        }
                                      , {
                                            name        => "Release Name"
                                          , x_percent   => 70
                                          , read_only   => 1
                                        }
                                      , {
                                            name        => "Release Open"
                                          , x_absolute  => 150
                                          , renderer    => "toggle"
                                          , read_only   => 1
                                        }
                                      , {
                                            name        => "Release Applied"
                                          , x_absolute  => 150
                                          , renderer    => "toggle"
                                          , read_only   => 1
                                        }
                                       ]
          , vbox                    => $self->{builder}->get_object( 'releases_box' )
          , auto_tools_box          => TRUE
          , recordset_tool_items    => [ "import" , "insert" , "delete_both" , "apply_release" , "package" , "diff" ]
          , recordset_extra_tools   => {
                                            import => {
                                                  type        => 'button'
                                                , markup      => "<span color='purple'>import</span>"
                                                , icon_name   => 'document-open'
                                                , coderef     => sub { $self->import_releases }
                                            }
                                          , delete_both => {
                                                  type        => 'button'
                                                , markup      => "<span color='red'>delete</span>"
                                                , icon_name   => 'edit-delete'
                                                , coderef     => sub { $self->delete_release }
                                            }
                                          , apply_release => {
                                                  type        => 'button'
                                                , markup      => "<span color='orange'><b>apply release</b></span>"
                                                , icon_name   => 'package-x-generic'
                                                , coderef     => sub { $self->apply_release_package }
                                            }
                                          , package => {
                                                  type        => 'button'
                                                , markup      => "<span color='blue'>package release</span>"
                                                , icon_name   => 'package-x-generic'
                                                , coderef     => sub { $self->package_release }
                                            }
                                          , diff => {
                                                  type        => 'button'
                                                , markup      => "<span color='green'>diff</span>"
                                                , icon_name   => 'format-indent-more'
                                                , coderef     => sub { $self->diff_releases }
                                            }
            }
          , on_row_select           => sub { $self->on_row_select( @_ ) }
          , on_insert               => sub { $self->on_row_insert( @_ ) }
        }
    );

    $self->{release_packages} = Gtk3::Ex::DBI::Datasheet->new(
        {
            dbh                     => $self->{globals}->{config_manager}->sdf_connection( "CONTROL" )
          , column_sorting          => 1
          , sql                     => {
                                            select      => "RELEASE_NAME , PACKAGE_TYPE , RELEASE_PACKAGE , OBJECT_CHANGED"
                                          , from        => "release_packages"
                                          , where       => "release_name = ?"
                                          , order_by    => "release_package"
                                          , bind_values => [ undef ]
                                       }
          , primary_keys            => [ "RELEASE_NAME", "RELEASE_PACKAGE" ]
          , force_upper_case_fields => 1
          , auto_incrementing       => 0
          , fields                  => [
                                        {
                                            name        => "release_name"
                                          , renderer    => "hidden"
                                        }
                                      , {
                                            name        => "Package Type"
                                          , x_absolute  => 180
                                        }
                                      , {
                                            name        => "Release Package"
                                          , x_percent   => 100
                                        }
                                      , {
                                            name        => "Object Changed"
                                          , x_absolute  => 150
                                          , renderer    => "toggle"
                                        }
                                       ]

          , vbox                    => $self->{builder}->get_object( 'release_packages_box' )
          , auto_tools_box          => TRUE
          , recordset_extra_tools   => {
                maybe_delete => {
                    type        => 'button'
                  , markup      => "<span color='red'>delete</span>"
                  , icon_name   => 'edit-delete'
                  , coderef     => sub { $self->maybe_delete_release_component }
                }
            }
          , recordset_tool_items    => [ qw ' maybe_delete ' ]
        }
    );

    $self->{tmp_dirs} = ();

    $self->import_releases();

    my $model = $self->{releases}->{treeview}->get_model;

    my $iter = $model->get_iter_first;
    my $applied_column = $self->{releases}->column_from_column_name( "Release Applied" );
    my $open_column    = $self->{releases}->column_from_column_name( "Release Open" );

    my $release_name_column = $self->{releases}->column_from_column_name( "Release Name" );

    my $do_requery;

    while ( $iter ) {

        if ( ! $model->get( $iter , $open_column ) && ! $model->get( $iter, $applied_column ) ) {
            if ( $options->{close_after_update_check} ) {
                $self->show();
            }
            my $release_name = $model->get( $iter, $release_name_column );
            my $answer;
            if ( $self->{globals}->{config_manager}->simpleGet( 'AUTO_APPLY_RELEASES' ) ) {
                $answer = 'yes';
            } else {
                $answer = $self->dialog(
                    {
                        title      => "Apply Release?"
                      , type       => "question"
                      , text       => "Apply new release: [ " . $release_name . " ]?"
                    }
                );
            }
            if ( $answer eq 'yes' ) {
                my $treeselection = $self->{releases}->{treeview}->get_selection;
                $treeselection->select_iter( $iter );
                $self->apply_release_package();
                $treeselection->unselect_all();
                $do_requery = 1;
            }
        }

        if ( ! $model->iter_next( $iter ) ) {
            last;
        }

    }

    if ( $do_requery ) {
        $self->{releases}->undo; # requery
    }

    if ( $options->{close_after_update_check} ) {
        Glib::Idle->add( sub {
                                $self->close_window();
                                return FALSE;
                             }
        );
    }

    return $self;
    
}

sub maybe_delete_release_component {

    my $self = shift;

    my @selected_open_flags = $self->{releases}->get_column_value( "RELEASE_OPEN" );
    my $this_open_flag      = $selected_open_flags[0];

    if ( ! $this_open_flag ) {

        $self->dialog(
            {
                title       => "Release closed"
              , type        => "error"
              , text        => "You can't delete objects from a closed release!"
            }
        );

        return;

    }

    $self->{release_packages}->delete;

}

sub delete_release {

    my $self = shift;

    my @selected_release_names = $self->{releases}->get_column_value( "RELEASE_NAME" );
    my $selected_release_name = $selected_release_names[0];

    $self->{globals}->{connections}->{CONTROL}->do(
        "delete from release_packages where release_name = ?"
      , [ $selected_release_name ]
    );

    $self->{releases}->delete;
    $self->{releases}->apply;
    $self->{releases}->query;
    $self->{release_packages}->query;

}

sub on_row_insert {
    
    my ( $self ) = @_;
    
    my $release_name = $self->dialog(
        {
            title       => "Enter a release name"
          , type        => "input"
          , text        => "Release names must be unique, and can't be changed later"
          , default     => $self->timestamp . "_<some_release>"
        }
    );
    
    my $repo = $self->dialog(
        {
            title       => "Select a repository"
          , type        => "options"
          , text        => "A release targets a single repository, and this can't be changed later"
          , options     => $self->{repos}
        }
    );
    
    if ( ! $repo ) {
        $self->{releases}->undo;
    }
    
    if ( ! $release_name ) {
        $self->{releases}->undo;
    }
    
    $self->{releases}->set_column_value( "RELEASE_OPEN", 1 );
    $self->{releases}->set_column_value( "RELEASE_NAME", $release_name );
    $self->{releases}->set_column_value( "REPOSITORY", $repo );

    $self->{releases}->apply;

}

sub on_row_select {
    
    my ( $self, $tree_selection ) = @_;

    my @selected_release_names = $self->{releases}->get_column_value( "RELEASE_NAME" );

    $self->{release_packages}->query(
        {
            bind_values     => [ $selected_release_names[$#selected_release_names] ]
        }
    );
    
}

sub diff_releases {

    my ( $self ) = @_;

    my @selected_release_names = $self->{releases}->get_column_value( "RELEASE_NAME" );
    my @selected_repos         = $self->{releases}->get_column_value( "REPOSITORY" );

    if ( @selected_release_names != 2 ) {

        $self->dialog(
            {
                title       => "Choose two"
              , type        => "info"
              , markup      => "The diff function requires you to select\n<b>more than 1</b> and <b>less than 3</b> releases"
            }
        );

        return;

    }

    my $base_release = $selected_release_names[0];
    my $this_release = $selected_release_names[1];
    my $base_repo    = $selected_repos[0];
    my $this_repo    = $selected_repos[1];

    my $tmp_dir = File::Temp->newdir();
    push @{$self->{tmp_dirs}}, $tmp_dir;

    mkdir $tmp_dir . "/base";
    mkdir $tmp_dir . "/base/template";
    mkdir $tmp_dir . "/base/job";

    mkdir $tmp_dir . "/release";
    mkdir $tmp_dir . "/release/template";
    mkdir $tmp_dir . "/release/job";

    foreach my $type ( qw | base release | ) {

        # Get a list of all the packages in this release
        my $this_release_packages = $self->{globals}->{connections}->{CONTROL}->select(
            "select package_type, object_name , release_package from release_packages where release_name = ?"
            , [ ( $type eq 'base' ? $base_release : $this_release ) ]
        );

        my $repo_dir = $self->{globals}->{config_manager}->repo_path(
            'gui'
            , ( $type eq 'base' ? $base_repo : $this_repo )
            , 'builtin'
        );

        foreach my $package_rec (@{$this_release_packages}) {

            print "copying " . $repo_dir . $package_rec->{PACKAGE_TYPE} . "/" . $package_rec->{RELEASE_PACKAGE} . "      ... to " . $tmp_dir . "/" . $type . "/" . $package_rec->{PACKAGE_TYPE} . "\n";

            copy(
                $repo_dir . $package_rec->{PACKAGE_TYPE} . "/" . $package_rec->{OBJECT_NAME} . "/" . $package_rec->{RELEASE_PACKAGE}
              , $tmp_dir . "/" . $type . "/" . $package_rec->{PACKAGE_TYPE} . "/" . $package_rec->{OBJECT_NAME} . ".json"
            ) || die( $! );

        }

    }

    system( "meld " . $tmp_dir . "/base " . $tmp_dir . "/release" );

}

sub package_release {

    my $self = shift;

    my @selected_release_names = $self->{releases}->get_column_value( "RELEASE_NAME" );

    if ( @selected_release_names != 1 ) {

        $self->dialog(
            {
                  title       => "Choose one"
                , type        => "info"
                , markup      => "You can only package 1 release at a time ..."
            }
        );

        return;

    }

    my @selected_open_flags = $self->{releases}->get_column_value( "RELEASE_OPEN" );
    my $this_open_flag      = $selected_open_flags[0];

    my @selected_repos      = $self->{releases}->get_column_value( "REPOSITORY" );
    my $this_repo           = $selected_repos[0];

    if ( ! $this_open_flag ) {

        $self->dialog(
            {
                title       => "Release already closed"
              , type        => "error"
              , markup      => "You can only package an <b><i>open</i></b> release"
            }
        );

        return;

    }

    my $this_release = $selected_release_names[0];

    my $closed_releases = $self->{globals}->{connections}->{CONTROL}->select(
        "select RELEASE_NAME from releases where RELEASE_OPEN = 0 and REPOSITORY = ? order by PACKAGE_DATETIME limit 5"
      , [ $this_repo ]
    );

    my $release_base_candidates = [];

    foreach my $release_rec ( @{$closed_releases} ) {
        push @{$release_base_candidates}, $release_rec->{RELEASE_NAME};
    }

    my $release_base;

    if ( @{$release_base_candidates} ) {

        $release_base = $self->dialog(
            {
                  title         => "Choose a base"
                , text          => "A release should be based on a previous release, which allows us to produce diffs and to rollback a release if necessary"
                , type          => "options"
                , options       => $release_base_candidates
                , orientation   => 'vertical'
            }
        );

    }

    $self->{globals}->{connections}->{CONTROL}->do(
        "insert into public.release_packages\n"
      . "(\n"
      . "    release_name\n"
      . "  , release_package\n"
      . "  , package_type\n"
      . "  , object_name\n"
      . "  , object_changed\n"
      . ") select\n"
      . "    ?\n"
      . "  , base.release_package\n"
      . "  , base.package_type\n"
      . "  , base.object_name\n"
      . "  , 0"
      . "from       release_packages base\n"
      . "left join  release_packages this_release\n"
      . "                                             on base.package_type = this_release.package_type\n"
      . "                                            and base.object_name  = this_release.object_name\n"
      . "                                            and this_release.release_name = ?\n"
      . "where\n"
      . "    base.release_name = ?\n"
      . "and this_release.release_name is null\n"
      , [
            $this_release
          , $this_release
          , $release_base
        ]
    );

    $self->{globals}->{connections}->{CONTROL}->do(
        "update releases set release_open = 0 , package_datetime = now() where release_name = ?"
      , [ $this_release ]
    );

    my $release = $self->{globals}->{connections}->{CONTROL}->select(
        "select repository , release_name , release_open , release_base , package_datetime from releases where release_name = ?" # skip the release_applied column
      , [ $this_release ]
    )->[0];

    my $release_packages = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from release_packages where release_name = ?"
      , [ $this_release ]
    );

    my $release_json = to_json(
        {
            release             => $release
          , release_packages    => $release_packages
        }
      , { pretty => 1 }
    );

    my $package_dir  = $self->{globals}->{config_manager}->repo_path(
        'gui'
      , $this_repo
      , ( $self->{globals}->{self}->{flatpak} ? 'persisted' : 'builtin' )
    );

    my $release_package_path = $package_dir . $this_release . ".json";

    eval {
        open FH, ">" . $release_package_path or die( "Can't open file:\n" . $! );
        print FH $release_json;
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

    $self->{releases}->query;

}

sub import_releases {

    my $self = shift;

    my $all_releases = $self->{globals}->{connections}->{CONTROL}->select(
        "select * from releases"
      , undef
      , "release_name"
    );

    foreach my $repo ( @{$self->{repos}} ) {

        say "Processing repo: [$repo]";

        my $package_dir  = $self->{globals}->{config_manager}->repo_path(
            'gui'
          , $repo
          , 'builtin'
        );

        say " Resolved to package dir: [$package_dir]";

        opendir( DIR, $package_dir ) || warn( $! );

        while ( my $file = readdir(DIR) ) {

            if ( $file =~ /(.*)\.json$/ ) {

                my ( $release_name ) = ( $1 );

                if ( ! $all_releases->{ $release_name } ) {

                    print "Importing [$release_name]\n";

                    my $release_package = $self->json_file_to_obj( $package_dir . "/" . $file );

                    $self->{globals}->{connections}->{CONTROL}->hash_to_table(
                        $release_package->{release}
                      , 'releases'
                    );

                    foreach my $package ( @{$release_package->{release_packages}} ) {
                        $self->{globals}->{connections}->{CONTROL}->hash_to_table(
                            $package
                          , 'release_packages'
                        );
                    }

                } else {

                    print "[$release_name] already imported into release manager in this environment - skipping\n";

                }

            }

        }

        close DIR;

    }

    $self->{releases}->query;
    $self->{release_packages}->query;

}

sub apply_release_package {

    my ( $self ) = @_;

    my @selected_release_names = $self->{releases}->get_column_value( "RELEASE_NAME" );

    if ( @selected_release_names != 1 ) {

        $self->dialog(
            {
                title    => "Select ONE release"
              , type     => "error"
              , text     => "Please select more than 0 and less then 2 releases"
            }
        );

        return;

    }
    
    my $apply_only_changed;
    
    if ( $self->{globals}->{config_manager}->simpleGet( 'INCREMENTAL_DIFF_WHEN_APPLYING_RELEASES') ) {

        $apply_only_changed = 'yes';

    } else {
        $apply_only_changed = $self->dialog(
            {
                 title     => "Incremental diff?"
               , type      => "question"
               , text      => "Answering yes will only apply objects that changed.\n"
                            . "Answering no will apply an entire 'release snapshot'. Do *not* do this when cherry-picking"
            }
        );
    }

    my $release_name = $selected_release_names[0];

    my @selected_repos = $self->{releases}->get_column_value( "REPOSITORY" );
    my $repo = $selected_repos[0];

    my $release_packages_sql = "select * from release_packages where release_name = ?";

    if ( $apply_only_changed eq 'yes' ) {
        $release_packages_sql .= " and object_changed = 1";
    }

    my $release_components = $self->{globals}->{connections}->{CONTROL}->select(
        $release_packages_sql
      , [ $release_name ]
    );

    if ( $self->{globals}->{connections}->{CONTROL}->can_ddl_in_transaction ) {
        $self->{globals}->{connections}->{CONTROL}->begin_work;
    }

    eval {

        foreach my $component_record (@{$release_components}) {

            my $component = $self->json_file_to_obj(
                $self->{globals}->{config_manager}->repo_path(
                    'gui'
                  , $repo
                  , 'builtin'
                ) . $component_record->{PACKAGE_TYPE} . "/" . $component_record->{OBJECT_NAME} . "/" . $component_record->{RELEASE_PACKAGE}
            );

            # The keys to component are all tables ...
            foreach my $table ( keys %{ $component } ) {

                my $table_obj = $component->{ $table };

                # Apply any DDL first ...
                if ( exists $table_obj->{ddl} ) {
                    foreach my $ddl ( @{$table_obj->{ddl}} ) {
                        $self->{globals}->{connections}->{CONTROL}->do( $ddl ) or die( "execute ddl failed" );
                    }
                }

                # Then data ...
                #  - pre - these are DML that are run *prior* to loading the JSON data
                if ( exists $table_obj->{pre} ) {
                    foreach my $dml ( @{$table_obj->{pre}} ) {
                        $self->{globals}->{connections}->{CONTROL}->do( $dml ) or die( "execute pre failed" );
                    }
                }

                # - data - json-encoded data
                if ( exists $table_obj->{data} ) {
                    foreach my $json_record ( @{$table_obj->{data}} ) {
                        $self->{globals}->{connections}->{CONTROL}->hash_to_table(
                            $json_record
                          , $table
                        )  or die( "execute data failed" );
                    }
                }

                # - post - DML that are run *post* loading the JSON data
                if ( exists $table_obj->{post} ) {
                    foreach my $dml ( @{$table_obj->{post}} ) {
                        $self->{globals}->{connections}->{CONTROL}->do( $dml ) or die( "execute post failed" );
                    }
                }

            }

        }

    };

    my $err = $@;

    if ( $err ) {

        warn $err;

        if ( $self->{globals}->{connections}->{CONTROL}->can_ddl_in_transaction ) {

            $self->dialog(
                {
                    title       => "Release failed"
                  , type        => "error"
                  , text        => "Detected an error during the release. You should have already seen an error dialog detailing the issue. Rolling back transaction ...\n\nError captured was:\n$err"
                }
            );

            $self->{globals}->{connections}->{CONTROL}->rollback;

            return;

        } else {

            $self->dialog(
                {
                    title       => "Release failed"
                  , type        => "error"
                  , text        => "Detected an error during the release. Additionally, you're hosting metadata on a database that doesn't support DDLs inside a transaction, so can can't roll back. You might want to try applying the previous release. Good luck with that :)"
                }
            );

            return;

        }

    }

    if ( $self->{globals}->{connections}->{CONTROL}->can_ddl_in_transaction ) {
        $self->{globals}->{connections}->{CONTROL}->commit;
    }

    $self->{globals}->{connections}->{CONTROL}->do(
        "insert into release_log ( release_datetime , release_name ) values ( now() , ? )"
      , [ $release_name ]
    );

    $self->{globals}->{connections}->{CONTROL}->do(
        "update releases set release_applied = 1 where release_name = ?"
        , [ $release_name ]
    );

    my ( $selected_paths, $model ) = $self->{releases}->{treeview}->get_selection->get_selected_rows;

    my $iter;

    for my $path ( @{$selected_paths} ) {
        $iter = $model->get_iter($path);
        $model->set( $iter , $self->{releases}->column_from_column_name( "Release Applied" ) , 1 );
        $model->set( $iter , 0 , 0 );
    }

    $self->pulse( "The release [ $release_name ] was completed successfully" );

}

sub json_file_to_obj {

    my ( $self, $file ) = @_;

    open RELEASE, "<" . $file
        or die( "Failed to open release package [$file]\n" . $! );

    local $/; # read entire file

    my $release_json = <RELEASE>;

    close RELEASE;

    my $release_package = decode_json( $release_json );

    return $release_package;

}

sub on_release_manager_destroy {
    
    my $self = shift;

    $self->{tmp_dirs} = undef;

    $self->close_window();
    
}

1;
