--($args.pairs blob, $args.active integer)--

select
    u.user_id as id,
    u.user_name as name
from users u
where optional_groups_or({{$args.pairs}}, u.user_id in ({{ids}}))
  and optional(u.active = {{$args.active}})
order by u.user_id
