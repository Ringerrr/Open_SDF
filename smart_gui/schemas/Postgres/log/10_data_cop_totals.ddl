create table data_cop_totals
(
    job_id              bigint          not null
  , database_name       varchar(100)    not null
  , schema_name         varchar(100)    not null
  , table_name          varchar(100)    not null
  , records_validated   bigint          not null
  , records_rejected    bigint          not null
);
