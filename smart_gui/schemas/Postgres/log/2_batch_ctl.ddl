CREATE TABLE  BATCH_CTL
(
     BATCH_ID                  bigint                    not null
  ,  BATCH_IDENTIFIER          character varying(100)
  ,  START_TS                  timestamp
  ,  END_TS                    timestamp
  ,  STATUS                    character varying(50)
  ,  PROCESSING_TIME           bigint
  ,  HOSTNAME                  character varying(50)
  ,  BATCH_ARGS                character varying(8000)

  ,  primary key ( BATCH_ID )
)
;
