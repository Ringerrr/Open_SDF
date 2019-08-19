create table RELEASES
(
    REPOSITORY              varchar(200)
  , RELEASE_NAME            varchar(200)
  , RELEASE_OPEN            smallint

  , primary key ( RELEASE_NAME )
);
