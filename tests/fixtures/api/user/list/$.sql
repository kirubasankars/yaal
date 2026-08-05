--($args.active integer)--

select
    u.user_id,
    u.user_name,
    u.active
from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
order by u.user_id
