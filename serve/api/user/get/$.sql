--($path.id integer)--

--sql--

select
    *
from
    user
INNER JOIN
    role
INNER JOIN
    user_role ur on ur.user_id = user.user_id and ur.role_id = role.role_id
WHERE
    user.active = 1 and role.active = 1 and ({{$path.id}} is null or user.user_id = {{$path.id}})
ORDER BY
    user.user_id