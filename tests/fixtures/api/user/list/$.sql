--($args.active integer, $args.sort string = id, $args.dir string = asc)--

select
    u.user_id,
    u.user_name,
    u.active
from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}}),
  u.user_id asc
