with user_group_log as (
    select
    	luga.hk_group_id,
    	count(sah.user_id_from) as cnt_added_users
    from STV202312114__DWH.s_auth_history as sah
    left join STV202312114__DWH.l_user_group_activity as luga on sah.hk_l_user_group_activity = sah.hk_l_user_group_activity
    where sah.event = 'add'
    group by 1
)
select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10