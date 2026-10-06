--($args.pairs blob, $args.active integer)--

select
    u.user_id as id,
    u.user_name as name
from users u
where u.user_id > 0
  and optional(u.active = {{$args.active}}
               and optional_groups_or({{$args.pairs}}, u.user_id in ({{ids}})))
order by u.user_id
