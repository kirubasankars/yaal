--($args.pairs blob)--

select * from (select 1 as id union select 2) t
where optional_groups_or({{$args.pairs}}, id = {{id}})
