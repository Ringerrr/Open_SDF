CREATE TABLE  JOB_CTL
(
     JOB_ID                    bigint
  ,  BATCH_ID                  bigint                        references BATCH_CTL(BATCH_ID)
  ,  IDENTIFIER                character varying(200)
  ,  PROCESSING_GROUP_NAME     character varying(200)
  ,  JOB_ARGS                  character varying(8000)
  ,  EXTRACT_TS                timestamp
  ,  START_TS                  timestamp
  ,  END_TS                    timestamp
  ,  STATUS                    character varying(50)
  ,  PROCESSING_TIME           bigint
  ,  HOSTNAME                  character varying(50)
  ,  ERROR_MESSAGE             character varying(20000)
  ,  LOG_PATH                  character varying(2000)

  ,  primary key ( JOB_ID )
)
;
