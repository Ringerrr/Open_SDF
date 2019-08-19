CREATE TABLE  LOAD_EXECUTION
(
     LOAD_EXECUTION_ID         bigint
  ,  JOB_ID                    bigint                         references JOB_CTL(JOB_ID)
  ,  PROCESSING_GROUP_NAME     character varying(200)
  ,  SEQUENCE_ORDER            integer
  ,  TARGET_DB_NAME            character varying(50)
  ,  TARGET_SCHEMA_NAME        character varying(50)
  ,  TARGET_TABLE_NAME         character varying(50)
  ,  START_TS                  timestamp
  ,  END_TS                    timestamp
  ,  PROCESSING_TIME           bigint
  ,  EXECUTION_STS             character varying(200)
  ,  ROWS_AFFECTED             bigint
  ,  HOSTNAME                  character varying(50)

  ,  primary key (LOAD_EXECUTION_ID)
)
;
