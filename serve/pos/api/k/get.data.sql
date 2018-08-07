--params($parent.page integer)--

select 1 as "$error", "page can't be less then one" as message 
where {{$parent.page}} < 1

--query()--

select * from customer order by id limit ({{$parent.page}} - 1) * 10, 10