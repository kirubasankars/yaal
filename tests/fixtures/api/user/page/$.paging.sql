--($args.page integer, $args.page_size integer, $params.total_count integer)--

SELECT
    'params' AS "$mode",
    COUNT(*) AS total_count
FROM users
WHERE active = 1

--sql--

SELECT
    {{$args.page}} AS page,
    {{$args.page_size}} AS page_size,
    {{$params.total_count}} AS total_count
