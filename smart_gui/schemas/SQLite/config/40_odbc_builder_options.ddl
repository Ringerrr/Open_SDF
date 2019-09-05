create table odbc_driver_options
(
    ID             integer       primary key
  , Driver         text
  , OptionName     text
  , OptionValue    text
  , Help           text
  , Source         text
);
