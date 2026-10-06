--($args.apply bool, $args.id integer)--

select
    u.user_id as id,
    u.user_name as name
from users u
where u.user_id > 0
  and optional_when({{$args.apply}}, u.user_id = {{$args.id}})
order by u.user_id
