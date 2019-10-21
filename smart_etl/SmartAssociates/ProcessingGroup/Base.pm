package SmartAssociates::ProcessingGroup::Base;

use strict;
use warnings;

use base 'SmartAssociates::Base';

my $IDX_TOP_LEVEL_TEMPLATES                     =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 0;
my $IDX_TARGET_DATABASES                        =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 1;
my $IDX_DBH                                     =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 2;
my $IDX_PROCESSING_GROUP_NAME                   =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 3;
my $IDX_ROOT_STEP_NUMBER                        =  SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 4;

use constant    FIRST_SUBCLASS_INDEX            => SmartAssociates::Base::FIRST_SUBCLASS_INDEX + 5;

# Base class of processing groups. Everything ( at the moment )
# other than Ingestion uses the base class

sub generate {
    
    # This STATIC FUNCTION ( not a method ) will determine which subclass of SmartAssociates::Database::Item::Job
    # we need, and construct an object of that type
    
    my $globals                 = $_[0];
    
    my $connection_name         = 'METADATA';
    my $connection_class        = $globals->CONNECTION_NAME_TO_DB_TYPE( $connection_name );
    
    my $processing_group_class  = 'SmartAssociates::ProcessingGroup::' . $connection_class;

    my $processing_group_object = SmartAssociates::Base::generate(
                                      $globals
                                    , $processing_group_class
                                  );
    
    return $processing_group_object;
    
}

sub new {
    
    my $self   = $_[0]->SUPER::new( $_[1] );
    
    $self->[ $IDX_DBH ] = SmartAssociates::Database::Connection::Base::generate(
        $self->globals
      , 'METADATA'
      , $self->globals->CONTROL_DB_NAME
    );
    
    $self->[ $IDX_TOP_LEVEL_TEMPLATES ] = [];
    $self->[ $IDX_TARGET_DATABASES ]    = {};
    
    # NOTE: if you add more args to the constructor, you MUST alter subclasses accordingly ...
    
    return $self;
    
}

sub prepare {
    
    my $self = shift;
    
    $self->getMetadata();
    $self->getRegisteredTemplates();
    $self->globals->JOB->prepare();
    
}

sub getMetadata {
    
    my $self = shift;
    
    my $CONTROL_DB_NAME = $self->globals->CONTROL_DB_NAME;
    my $LOG_DB_NAME     = $self->globals->LOG_DB_NAME;
    
    my $sth = $self->[ $IDX_DBH ]->prepare(
        "select\n"
      . "    PROCESSING_GROUP_NAME\n"
      . "from\n"
      . "            " . $LOG_DB_NAME . "..JOB_CTL\n"
      . "where\n"
      . "    JOB_CTL.JOB_ID = ?\n"
    );
    
    $self->[ $IDX_DBH ]->execute(
        $sth
      , [
            $self->globals->JOB->key_value
        ]
    );
    
    my $rec = $sth->fetchrow_hashref;
    
    $self->[ $IDX_PROCESSING_GROUP_NAME ] = $rec->{PROCESSING_GROUP_NAME};
    
    $sth->finish();
    
}

sub getRegisteredTemplates {
    
    my $self = shift;

    # Here we build a hierarchy of templates to execute

    # If a template is defined as an iterator, then we search for templates
    # underneath it.

    # If we're passed a root step id via json-encoded job args, we only
    # fetch steps underneath that root step.
    #
    # First we get a list of template ID, load config IDs, and classes,
    # in the order we're going to execute them in ...

    my $bind_values = [ $self->[ $IDX_PROCESSING_GROUP_NAME ] ];

    my $sql = "select\n"
     . "    CONFIG.SEQUENCE_ORDER\n"
     . "  , CONFIG.PARENT_SEQUENCE_ORDER\n"
     . "  , TEMPLATE.TEMPLATE_NAME\n"
     . "  , TEMPLATE.CLASS\n"
     . "from\n"
     . "             CONFIG\n"
     . "inner join   TEMPLATE\n"
     . "    on\n"
     . "            CONFIG.TEMPLATE_NAME                = TEMPLATE.TEMPLATE_NAME\n"
     . "where\n"
     . "    CONFIG.PROCESSING_GROUP_NAME = ?\n"
     . "and CONFIG.DISABLE_FLAG = 0\n";

     my $root_step_id = $self->globals->JOB->job_arg( 'ROOT_STEP_ID' );

     if ( $root_step_id ) {
         $sql .= "and parent_sequence_order = ?\n";
         push @{$bind_values} , $root_step_id;
     }

     $sql .= "order by\n"
     . "    CONFIG.SEQUENCE_ORDER";

    my $sth = $self->[ $IDX_DBH ]->prepare( $sql );

    $self->[ $IDX_DBH ]->execute(
        $sth
      , $bind_values
    );

    # Then we loop through the list, and construct the appropriate objects to
    # do the actual work, based on the TEMPLATE.CLASS
    
    my $templates_by_sequence = {};
    
    # We temporarily store a hash of templates by config id, so we can
    # easily attach children to them while creating our hierarchy ...
    
    while ( my $template_rec = $sth->fetchrow_hashref ) { 
        
        my $class = 'SmartAssociates::TemplateConfig::' . $template_rec->{CLASS};

        my $template = SmartAssociates::Base::generate(
            $self->globals
          , $class
          , $self
          , $template_rec->{SEQUENCE_ORDER}
        );

        $templates_by_sequence->{ $template_rec->{SEQUENCE_ORDER} } = $template;
        
        if ( ! $template_rec->{PARENT_SEQUENCE_ORDER}
            || $template_rec->{PARENT_SEQUENCE_ORDER} == $root_step_id ) {
            
            # top-level templates ( or for forked children, templates directly under their root step id )
            # go straight onto the array of top-level templates
            push @{$self->[ $IDX_TOP_LEVEL_TEMPLATES ]}, $template;
            
        } else {
            
            # For child templates, we locate them via their ID in our mapping
            # Note that it should be safe to do this here ( as opposed to waiting until
            # we've created all templates ) because we're ordering by SEQUENCE_ORDER, which
            # is how things will be executed, so the parent will already have been created 

            if ( exists $templates_by_sequence->{ $template_rec->{PARENT_SEQUENCE_ORDER} } ) {
                $templates_by_sequence->{ $template_rec->{PARENT_SEQUENCE_ORDER} }->add_child_template_config( $template );
            } else {
                $self->log->warn( "Parent step number [" . $template_rec->{PARENT_SEQUENCE_ORDER} . "] not loaded - probably out of scope ( ie we're a forked child process )" );
            }
            
        }
        
    }
    
    $sth->finish();
    
}

sub target_database {
    
    my ( $self, $connection_name, $target_database_name ) = @_;
    
    if ( ! exists $self->[ $IDX_TARGET_DATABASES ]->{ $connection_name . ':' . $target_database_name }
      || !        $self->[ $IDX_TARGET_DATABASES ]->{ $connection_name . ':' . $target_database_name }
    ) {
        
        my $target_database_type = $self->globals->CONNECTION_NAME_TO_DB_TYPE( $connection_name );
        
        my $class = 'SmartAssociates::Database::Connection::' . $target_database_type;

        my $target_db = SmartAssociates::Base::generate(
            $self->globals
          , $class
          , $connection_name
          , $target_database_name
        );
        
        # we don't expect / support named placeholders in template sql, and this feature breaks
        # some sql ( eg the lazy form of casting: select som_col::VARCHAR )
        $target_db->dbh->{odbc_ignore_named_placeholders} = 1;
        
        $self->[ $IDX_TARGET_DATABASES ]->{ $connection_name . ':' . $target_database_name } = $target_db;
        
    }
    
    return $self->[ $IDX_TARGET_DATABASES ]->{ $connection_name . ':' . $target_database_name };
    
}

sub rollback_target_databases {
    
    my $self = shift;
    
    foreach my $target_database_key ( keys %{ $self->[ $IDX_TARGET_DATABASES ] } ) {
        
        eval {
            $self->[ $IDX_TARGET_DATABASES ]->{ $target_database_key }->rollback();
            delete $self->[ $IDX_TARGET_DATABASES ]->{ $target_database_key };
        };
        
    }
    
}

sub commit_target_databases {
    
    my $self = shift;
    
    foreach my $target_database_key ( keys %{ $self->[ $IDX_TARGET_DATABASES ] } ) {
        
        $self->[ $IDX_TARGET_DATABASES ]->{ $target_database_key }->commit();
        
    }
    
}

sub execute {
    
    my $self = shift;
    
    $self->globals->JOB->begin();
    
    # NOTE: this executes all TOP-LEVEL templates.
    # Iterators execute all other templates ( see SmartAssociates::TemplateConfig::Base::complete )
    
    foreach my $template ( @{$self->[ $IDX_TOP_LEVEL_TEMPLATES ]} ) {
        # we store this so Log.pm can easily get at it to decide whether to downgrade fatals if ON_ERROR_CONTINUE is set
        # note that completing steps with iterating child steps also set this
        $self->globals->CURRENT_TEMPLATE_CONFIG( $template );
        $template->prepare();
        eval {
            $template->execute();
        };
        my $err = $@;
        if ( $err ) {
            if ( ! $template->on_error_continue ) {
                $self->log->fatal( $err );
            } else {
                $self->log->info( "Caught an error while executing a step, but ON_ERROR_CONTINUE was set. Continuing ..." );
            }
        }
        $template->complete();
    }
    
}

sub complete {
    
    my $self = shift;
    
    my $job = $self->globals->JOB;
    $job->field( SmartAssociates::Database::Item::Job::Base::FLD_STATUS, SmartAssociates::Database::Item::Job::Base::STATUS_COMPLETE );
    $job->update();
    
}

sub dbh                         { return $_[0]->accessor( $IDX_DBH,                         $_[1] ); }
sub name                        { return $_[0]->accessor( $IDX_PROCESSING_GROUP_NAME,       $_[1] ); }
sub processing_group_name       { return $_[0]->accessor( $IDX_PROCESSING_GROUP_NAME,       $_[1] ); } # We need this name as well as the above

1;
