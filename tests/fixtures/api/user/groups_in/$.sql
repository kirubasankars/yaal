--($args.pairs blob)--

select
    u.user_id as id,
    u.user_name as name
from users u
where optional_groups_or({{$args.pairs}}, u.user_id in ({{ids}}))
order by u.user_id
