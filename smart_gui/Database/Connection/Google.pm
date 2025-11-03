package Database::Connection::Google;

use parent 'Database::Connection';

use strict;
use warnings;

use Glib qw | TRUE FALSE |;

use Exporter qw ' import ';

use constant DB_TYPE    => 'Google';

# Subclasses must override connection_label_map() and connect_do()

sub auth_config {
    my $self = shift;
    if ( @_ ) {
        $self->{auth_config} = shift;
    }
    return $self->{auth_config};
}

sub bucket_cache {
    my $self = shift;
    if ( !defined $self->{bucket_cache} ) {
        $self->{bucket_cache} = {};
    }
    return $self->{bucket_cache};
}

sub _get_bucket_client {

    my ( $self, $bucket_name ) = @_;

    # Return cached bucket client if available
    my $cache = $self->bucket_cache();
    if ( exists $cache->{ $bucket_name } ) {
        return $cache->{ $bucket_name };
    }

    my $auth_config = $self->auth_config();

    require lib;
    lib->import('/Users/dankasak/src/perl_gcs');
    require Google::Cloud::Storage::Bucket;

    my $params = {
        bucket_name => $bucket_name,
        %{$auth_config}
    };

    my $bucket = Google::Cloud::Storage::Bucket->new( $params );

    # Cache the bucket client
    $cache->{ $bucket_name } = $bucket;

    return $bucket;

}

sub fetch_database_list {

    my $self = shift;

    # Google Cloud Storage is bucket-scoped, not project-scoped
    # We cannot easily list buckets without project-level API access
    # This would require a different API endpoint

    return ();

}

sub can_execute_ddl {

    my $self = shift;

    return FALSE;

}

sub has_odbc_driver {

    my $self = shift;

    return FALSE;

}

1;
