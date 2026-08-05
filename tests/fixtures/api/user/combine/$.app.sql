--($args.id integer)--

SELECT
    u.user_id,
    u.user_name
FROM users u
WHERE u.user_id = {{$args.id}}
