--params($parent.page integer)--

SELECT 
    * 
FROM 
    (select * from film order by film_id limit 10 offset ({{$parent.page}} - 1) * 10) f 
LEFT JOIN
    film_actor fa ON fa.film_id = f.film_id
LEFT JOIN 
    actor a ON fa.actor_id = a.actor_id
order by f.film_id