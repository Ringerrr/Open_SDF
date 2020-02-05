create table odbc_drivers
(
    ID             integer       primary key
  , Driver         text
  , Configured     integer       default 0
);
