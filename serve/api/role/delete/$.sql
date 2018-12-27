--($path.id string)--

delete from role where role_id = {{$path.id}}

--sql--

select 'true' as 'ok'