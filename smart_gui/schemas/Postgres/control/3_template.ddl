CREATE TABLE TEMPLATE (
    TEMPLATE_NAME    CHARACTER VARYING(200)     not null
  , TEMPLATE_DESC    CHARACTER VARYING(500)
  , TEMPLATE_TEXT    CHARACTER VARYING(8000)    not null
  , CLASS            CHARACTER VARYING(200)     not null

  , primary key ( TEMPLATE_NAME )
);