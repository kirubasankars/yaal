--($request.id string, name string)--

insert into role (role_id, role_name) values({{$request.id}}, {{name}})

--sql--

select 'true' as 'ok'