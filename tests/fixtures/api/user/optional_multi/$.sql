--($args.active integer, $args.ids integer[], $args.name string)--

select
    u.user_id as id,
    u.user_name as name,
    u.active as active
from users u
where optional(
    u.active = {{$args.active}}
    and u.user_id in ({{$args.ids}})
    and u.user_name = {{$args.name}}
)
order by u.user_id
