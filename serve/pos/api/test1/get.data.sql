--params($parent.page integer, name string)--

select "Kiruba" as name, 1 as "$params"

--query()--

select *, {{name}} as name from customer order by Id LIMIT ({{$parent.page}} - 1) * 10, 10;