create table if not exists releases (
   ID               integer primary key
 , Path             text
 , ReleaseName      text
 , RollbackRelease  text
)
