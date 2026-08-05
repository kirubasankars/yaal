WITH role_counts AS (
    SELECT
        ur.user_id,
        COUNT(*) AS role_count
    FROM user_roles ur
    GROUP BY ur.user_id
)
SELECT
    COUNT(*) AS user_count,
    SUM(CASE WHEN u.active = 1 THEN 1 ELSE 0 END) AS active_count,
    COALESCE(SUM(rc.role_count), 0) AS assignment_count
FROM users u
LEFT JOIN role_counts rc ON rc.user_id = u.user_id
