--(id! integer, name! string)--

INSERT INTO users (user_id, user_name, active) VALUES ({{id}}, {{name}}, 1)

--sql--

INSERT INTO user_roles (user_id, role_id) VALUES ({{id}}, 2)

--sql--

SELECT
    u.user_id,
    u.user_name,
    r.role_id,
    r.role_name
FROM users u
INNER JOIN user_roles ur ON ur.user_id = u.user_id
INNER JOIN roles r ON r.role_id = ur.role_id
WHERE u.user_id = {{id}}
ORDER BY r.role_id
