use DBI;

foreach (@{ $DBI::EXPORT_TAGS{sql_types} }) {
    
    my $str = sprintf "%s=%d\n", $_, &{"DBI::$_"};
    
    my ( $code , $type );
    if ( $str =~ /(.*)=(.*)/ ) {
        $code = $2;
        $type = $1;
    } else {
        die( "Failed to parse: $str" );
    }
    
    print "      , $code    => 'TEXT'    # $type\n";
    
}
