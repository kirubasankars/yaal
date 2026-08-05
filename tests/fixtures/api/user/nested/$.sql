--($args.id integer)--

select
    u.user_id,
    u.user_name
from users u
where u.active = 1
  and optional(u.user_id = {{$args.id}})
order by u.user_id
