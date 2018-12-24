--($path.id integer)--

select
    permission_id as id, permission_name as name
from
    permission
where
    ({{$path.id}} is null or permission.permission_id = {{$path.id}})
order by
    permission_id