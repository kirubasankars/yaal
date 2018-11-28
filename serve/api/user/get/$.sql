--($query.id integer)--

select * from customer where ({{$query.id}} is null or customer_id = {{$query.id}})