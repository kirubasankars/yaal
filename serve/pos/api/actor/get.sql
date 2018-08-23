--($query.id integer)--

select * from actor where {{$query.id}} is null or actor_id = {{$query.id}};
