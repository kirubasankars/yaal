--params($parent.page integer)--

SELECT * FROM address a join city c on a.city_id = c.city_id order by address_id limit 10 offset ( {{$parent.page}} - 1 ) * 10
