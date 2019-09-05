create table odbc_driver_options
(
    type         text
  , option_name  text
  , option_value text
  , help         text

  , primary key ( type , option_name )
);
