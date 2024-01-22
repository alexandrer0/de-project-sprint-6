with user_group_messages as (
    select
    	hg.hk_group_id,
    	count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV202312114__DWH.h_groups hg
    left join STV202312114__DWH.l_groups_dialogs lgd on hg.hk_group_id = lgd.hk_group_id
    left join STV202312114__DWH.l_user_message lum on lum.hk_message_id = lgd.hk_message_id
    group by 1
)
select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10