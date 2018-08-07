--params($parent.page integer, $parent.total_pages integer)--

SELECT 1 as "$error", "page can't be less then or equal to zero or more then " 
|| {{$parent.total_pages}} as message
 where {{$parent.page}} <= 0 or {{$parent.page}} > {{$parent.total_pages}}

--query()--

SELECT * FROM Customer ORDER BY Id LIMIT ({{$parent.page}} - 1) * 10, 10