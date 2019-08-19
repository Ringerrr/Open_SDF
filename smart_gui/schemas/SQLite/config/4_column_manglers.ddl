create table if not exists column_manglers (
    ID             integer primary key autoincrement
  , regex_find     text
  , regex_replace  text
  , description    text
  , active         integer
  , exe_sequence   integer
)