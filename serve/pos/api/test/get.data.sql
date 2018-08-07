--params($parent.page integer)--

SELECT 1 as "$error", "page can't be less or equal to zero." as message where {{$parent.page}} <= 0

--query()--

SELECT * FROM Customer ORDER BY Id LIMIT ({{$parent.page}} - 1) * 10, 10


--query()--