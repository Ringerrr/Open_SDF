CREATE TABLE  BROADWAY_SESSIONS
(
     USERNAME                  character varying(50)
  ,  START_DATE                timestamp
  ,  END_DATE                  timestamp
  ,  DISPLAY                   character varying(4)
  ,  PORT                      integer
  ,  BROADWAY_PID              bigint
  ,  GUI_PID                   bigint

  ,  primary key ( USERNAME , START_DATE )
)
;
