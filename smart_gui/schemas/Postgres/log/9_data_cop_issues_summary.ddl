create table data_cop_issues_summary
(
    job_id             bigint        not null
  , database_name      varchar(100)  not null
  , schema_name        varchar(100)  not null
  , table_name         varchar(100)  not null
  , column_name        varchar(100)  not null
  , column_type        varchar(100)  not null
  , column_type_extra  varchar(100)
  , issue_type         varchar(100)  not null
  , issue_count        bigint        not null
);
