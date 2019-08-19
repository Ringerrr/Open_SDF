CREATE TABLE HARVEST_CONTROL (
    FILE_REGEX               CHARACTER VARYING(1000)
  , PROCESSING_GROUP_NAME    CHARACTER VARYING(200)
  , POS_1                    CHARACTER VARYING(50)
  , POS_2                    CHARACTER VARYING(50)
  , POS_3                    CHARACTER VARYING(50)
  , POS_4                    CHARACTER VARYING(50)
  , POS_5                    CHARACTER VARYING(50)
  , POS_6                    CHARACTER VARYING(50)
  , POS_7                    CHARACTER VARYING(50)
  , POS_8                    CHARACTER VARYING(50)
  , POS_9                    CHARACTER VARYING(50)
  , POS_10                   CHARACTER VARYING(50)
  , POS_11                   CHARACTER VARYING(50)
  , POS_12                   CHARACTER VARYING(50)
  , POS_13                   CHARACTER VARYING(50)
  , POS_14                   CHARACTER VARYING(50)
  , POS_15                   CHARACTER VARYING(50)
  , GROUPING_CODE            CHARACTER VARYING(200)
  , PERFORM_LOAD_ORDER_CHECK SMALLINT                    not null    DEFAULT 0
  , EXAMPLE_FILENAME         CHARACTER VARYING(200)
  , RECORD_LENGTH            BIGINT

  , primary key ( PROCESSING_GROUP_NAME )
);
