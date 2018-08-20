--($query.page integer)--

SELECT * FROM category limit 10 offset ({{$query.page}} - 1) * 10
