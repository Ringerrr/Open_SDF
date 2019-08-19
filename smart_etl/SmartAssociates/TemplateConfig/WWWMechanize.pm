package SmartAssociates::TemplateConfig::WWWMechanize;

use strict;
use warnings;

use WWW::Mechanize;
use JSON;

use base 'SmartAssociates::TemplateConfig::Base';

use constant VERSION                            => '1.0';

sub execute {
    
    my $self = shift;
    
    my $template_config = $self->template_record;

    $self->log->info( "--  --  --  --  Starting template [" . $template_config->{TEMPLATE_NAME} . "] version [" . VERSION . "] --  --  --  --" );
    
    my $start_ts = $self->log->prettyTimestamp();
    
    # Resolve all the params we use
    
    # First the required ones ...
    my $url                         = $self->resolve_parameter( '#P_URL#' )                 || $self->log->fatal( "Missing param #P_URL#" );

    my $output_filename             = $self->resolve_parameter( '#P_OUTPUT_FILENAME#' );
    my $cookie_jar_name             = $self->resolve_parameter( '#P_COOKIE_JAR_NAME#' );
    my $form_fields_json            = $self->detokenize( '#P_FORM_FIELDS_JSON#' );

    my ( $mech, $cookie_jar, $form_fields );

    my $template_text               = $self->detokenize( $template_config->{TEMPLATE_TEXT} );

    eval {

        # If we've been passed a cookie jar name, fetch the cookie jar
        if ( $cookie_jar_name ) {
            $cookie_jar = $self->globals->MISC->{ $cookie_jar_name };
        }

        if ( ! $cookie_jar ) {
            $cookie_jar = {};
        }

        if ( $form_fields_json ) {
            $form_fields = decode_json( $form_fields_json );
        }

        $mech = WWW::Mechanize->new(
            cookie_jar      => $cookie_jar
          , autocheck       => 1
        );

        my $response = $mech->get( $url );

        $self->perf_stat_start( 'Download URL' );

        if ( ! $response->is_success ) {
            $self->perf_stat_stop( 'Download URL' );
            die "GET failed: ",  $response->status_line, "\n";
        } else {
            $self->perf_stat_stop( 'Download URL' );
        }

        if ( $form_fields ) {
            my @fields = keys %{$form_fields};
            my $form = $mech->form_with_fields( @fields );
            foreach my $field ( @fields ) {
                $mech->field( $field, $form_fields->{ $field } );
            }
            $self->perf_stat_start( 'Submit Form' );
            $response = $mech->click();
            if ( ! $response->is_success ) {
                $self->perf_stat_stop( 'Submit Form' );
                die( "CLICK failed: ", $response->status_line, "\n" );
            } else {
                $self->perf_stat_stop('Submit Form');
            }
        }

        if ( $output_filename ) {

            my $output;

            open $output, ">" . $output_filename
                    || die( "Failed to open output file [$output_filename] for writing:\n" . $! );

            print $output $response->content();

            close $output
                || die( "Failed to close output file:\n" . $! );

        }

    };

    if ( $cookie_jar_name ) {
        my $misc = $self->globals->MISC();
        $misc->{ $cookie_jar_name } = $mech->cookie_jar();
        $self->globals->MISC( $misc );
    }

    my $error = $@;
    
    my $end_ts = $self->log->prettyTimestamp();

    $self->globals->JOB->log_execution(
        $template_config->{PROCESSING_GROUP_NAME}
      , $template_config->{SEQUENCE_ORDER}
      , $url
      , undef
      , undef
      , $start_ts
      , $end_ts
      , 0
      , $error
      , $template_text
      , to_json( $self->perf_stats, { pretty => 1 } )
      , $template_config->{NOTES}
    );
    
    if ( $error ) {
        $self->log->fatal( $error );
    }
    
}

1;
