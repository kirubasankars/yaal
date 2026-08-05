--($args.id integer)--

select
    u.user_id,
    u.user_name,
    r.role_id,
    r.role_name
from users u
inner join user_roles ur on ur.user_id = u.user_id
inner join roles r on r.role_id = ur.role_id
where u.active = 1
  and r.active = 1
  and optional(u.user_id = {{$args.id}})
order by u.user_id, r.role_id
