CREATE TABLE connections (
    ID                integer primary key autoincrement
  , ConnectionName    text   not null
  , EnvironmentName   text
  , Username          text
  , Password          text
  , Host              text
  , Port              text
  , DatabaseType      text
  , ConnectionString  text
  , UseBuilder        number not null default 1
  , ProxyAddress      text
  , UseProxy          integer
  , IconName          text
  , Database          text
);

CREATE UNIQUE INDEX ConnectionsKey on connections(ConnectionName,EnvironmentName);

