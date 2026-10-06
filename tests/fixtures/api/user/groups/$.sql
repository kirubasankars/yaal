--($args.pairs blob)--

select * from (select 1 as id union select 2) t
where optional_groups({{$args.pairs}}, id = {{id}})
