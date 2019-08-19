CREATE TABLE broadway_user_auth(
        id                      serial     primary key
     ,  username                varchar(255) not null
     ,  password                varchar(255) not null
     ,  last_authenticated      timestamp
     ,  auth_key                varchar(255)
     ,  port                    int
     ,  display_number          int
)
;