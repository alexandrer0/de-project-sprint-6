with user_group_log as (
    select
    	luga.hk_group_id,
    	count(sah.user_id_from) as cnt_added_users
    from STV202312114__DWH.s_auth_history as sah
    left join STV202312114__DWH.l_user_group_activity as luga on sah.hk_l_user_group_activity = sah.hk_l_user_group_activity
    where sah.event = 'add'
    group by 1
)
,user_group_messages as (
    select
    	hg.hk_group_id,
    	count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV202312114__DWH.h_groups hg
    left join STV202312114__DWH.l_groups_dialogs lgd on hg.hk_group_id = lgd.hk_group_id
    left join STV202312114__DWH.l_user_message lum on lum.hk_message_id = lgd.hk_message_id
    group by 1
)
select
ugl.hk_group_id,
ugl.cnt_added_users,
ugm.cnt_users_in_group_with_messages,
ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc