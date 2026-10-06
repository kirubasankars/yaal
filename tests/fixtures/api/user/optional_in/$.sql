--($args.ids integer[])--

select
    u.user_id as id,
    u.user_name as name
from users u
where optional(u.user_id in ({{$args.ids}}))
order by u.user_id
