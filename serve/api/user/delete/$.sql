--($path.id string)--

DELETE FROM USER WHERE user_id = {{$path.id}}

--sql--

select 'true' as 'ok'