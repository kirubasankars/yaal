--($path.id integer)--

select
    role_id as id, role_name as name
from
    role
where
    ( {{$path.id}} is null or role.role_id = {{$path.id}})
order by
    role_id