--($args.id integer)--

--sql(flags)--

SELECT
    f.user_id,
    f.vip
FROM external_flags f
WHERE f.user_id = {{$args.id}}
