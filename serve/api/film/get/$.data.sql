--($query.page integer, $query.id integer)--

--sql--

SELECT 
    * 
FROM 
    (select * from film order by film_id limit 10 offset ({{$query.page}} - 1) * 10) f 
LEFT JOIN
    film_actor fa ON fa.film_id = f.film_id
LEFT JOIN 
    actor a ON fa.actor_id = a.actor_id
WHERE
    ({{$query.id}} is null or fa.film_id = {{$query.id}}) and (1=1)
order by f.film_id