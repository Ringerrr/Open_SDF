create table object_manglers (
    source_db_type        text
  , target_db_type        text
  , object_type           text
  , mangler_description   text
  , regex_search          text
  , regex_replace         text
  , case_sensitive        int              default 0
  , enabled               int              default 1

  , primary key ( source_db_type , target_db_type , object_type , mangler_description )
);
