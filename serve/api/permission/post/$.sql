--($request.id string, name string)--

insert into permission (permission_id, permission_name) values({{$request.id}}, {{name}})

--sql--

select 'true' as 'ok'