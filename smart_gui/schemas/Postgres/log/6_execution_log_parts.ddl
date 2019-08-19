CREATE TABLE  EXECUTION_LOG_PARTS
(
     LOAD_EXECUTION_ID         bigint                      not null
  ,  LOG_TYPE                  character varying(10)       not null
  ,  PART_SEQUENCE             integer                     not null
  ,  PART_TEXT                 character varying(60000)

  ,  primary key ( LOAD_EXECUTION_ID , LOG_TYPE , PART_SEQUENCE )
)
;
