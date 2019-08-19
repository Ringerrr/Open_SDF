create table job_full_log
(
     job_id                    bigint                         references job_ctl(job_id)
  ,  log_text                  text
  ,  primary key (job_id)
)
;
