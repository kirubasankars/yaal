--(active, sort, dir)--
select u.user_id, u.user_name
from users u
where 1 = 1
  and optional(u.active = {{active}})
order by
  sort({{sort}}, name = u.user_name, id = u.user_id)
  dir({{dir}})
