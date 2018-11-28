--($query.id integer)--

select * from customer where customer_id = {{$query.id}} order by customer_id
