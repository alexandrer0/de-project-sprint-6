drop table if exists STV202312114__STAGING.users;
drop table if exists STV202312114__STAGING.groups;
drop table if exists STV202312114__STAGING.dialogs;
drop table if exists STV202312114__STAGING.group_log;

create table STV202312114__STAGING.users(
    id int PRIMARY KEY,
    chat_name varchar(200),
    registration_dt timestamp,
    country varchar(200),
    age int
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;
;

create table STV202312114__STAGING.groups(
    id int PRIMARY KEY,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp,
    is_private boolean
)
ORDER BY id, admin_id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date,3,2)
;

create table STV202312114__STAGING.dialogs(
    message_id int PRIMARY KEY,
    message_ts timestamp,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int
)
ORDER BY message_id
SEGMENTED BY HASH(message_id) ALL NODES
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date,3,2)
;

create table STV202312114__STAGING.group_log(
    group_id int,
    user_id int,
    user_id_from int,
    event varchar(20),
    datetime timestamp
)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date,3,2)
;