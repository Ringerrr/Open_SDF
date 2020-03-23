package Database::Connection::AWS;

use parent 'Database::Connection';

use strict;
use warnings;

use Data::Dumper;

# use Net::Amazon::S3;
# use Net::Amazon::EC2;

use Glib qw | TRUE FALSE |;

use Exporter qw ' import ';

our @EXPORT_OK = qw ' UNICODE_FUNCTION LENGTH_FUNCTION SUBSTR_FUNCTION ';

use constant UNICODE_FUNCTION   => 'char2hexint';
use constant LENGTH_FUNCTION    => 'length';
use constant SUBSTR_FUNCTION    => 'substr';

use constant DB_TYPE            => 'BigQuery';

sub connection_label_map {

    my $self = shift;

    return {
        Username        => "AWS Access Key"
      , Password        => "Secret"
      , Database        => "Bucket Name"
      , Host_IP         => "Default Region"
      , Port            => ""
      , Attribute_1     => ""
      , Attribute_2     => ""
      , Attribute_3     => ""
      , Attribute_4     => ""
      , Attribute_5     => ""
      , ODBC_driver     => ""
    };

}

sub connect_do {

    my ( $self, $auth_hash, $options_hash ) = @_;

    eval {

        require Net::Amazon::S3;
        require Net::Amazon::EC2;

        # Attempt connection to AWS and retrieve bucket
        $self->{S3} = Net::Amazon::S3->new(
            {   aws_access_key_id     => $auth_hash->{Username},
                aws_secret_access_key => $auth_hash->{Password},
                retry                 => 1
            }
        );

        my $response = $self->{S3}->buckets
            or die $self->{S3}->err . ": " . $self->{S3}->errstr;
        
        print Dumper( $response );
        
        $self->{connection} = Net::Amazon::EC2->new(
            {   AWSAccessKeyId        => $auth_hash->{Username}
              , SecretAccessKey       => $auth_hash->{Password}
              , region                => $auth_hash->{Host}
            }
        );

    };

    my $err = $@;

    if ( $err ) {
        $self->dialog(
            {
                title   => "Failed to connect to AWS bucket"
              , type    => "error"
              , text    => $err
            }
        );
        return undef;
    }


    return 1;

}

sub fetch_database_list {
    
    my $self = shift;
    
    my $response = $self->{S3}->buckets();
    
    my @return;
    
    foreach my $bucket_obj ( @{ $response->{buckets} } ) {
        push @return, $bucket_obj->bucket;
    }
    
    return sort( @return );
    
}

sub get_instances {

    my $self = shift;

    my $instances = $self->{connection}->describe_instances();

    return $instances;

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
