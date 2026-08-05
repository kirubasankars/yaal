--($args.page integer, $args.page_size integer)--

SELECT
    u.user_id,
    u.user_name,
    r.role_id,
    r.role_name
FROM (
    SELECT user_id, user_name
    FROM users
    WHERE active = 1
    ORDER BY user_id
    LIMIT {{$args.page_size}} OFFSET ({{$args.page}} - 1) * {{$args.page_size}}
) u
INNER JOIN user_roles ur ON ur.user_id = u.user_id
INNER JOIN roles r ON r.role_id = ur.role_id
ORDER BY u.user_id, r.role_id
