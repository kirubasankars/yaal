--(id integer)--

select * from customers where ({{id}} is null or customerid = {{id}})
