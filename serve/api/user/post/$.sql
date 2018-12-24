--($request.id string, username string, password string)--

INSERT INTO user (user_id, user_name, password, active) VALUES ({{$request.id}}, {{username}}, {{password}}, 1)

--sql--

INSERT INTO user_role (user_id, role_id) VALUES({{$request.id}}, (select role_id from role where role_name = 'User' limit 1))

--sql--

select 'true' as 'ok'