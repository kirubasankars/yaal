--params($parent.page integer, name string)--

SELECT 1 as "$params", "Kiruba" as name;

--query()--

SELECT *, {{name}} as name FROM Customer Order by ID 
LIMIT ({{$parent.page}} - 1) * 10, 10 
