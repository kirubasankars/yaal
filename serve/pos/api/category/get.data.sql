--params($parent.page integer)--

SELECT * FROM category limit 10 offset ({{$parent.page}} - 1) * 10