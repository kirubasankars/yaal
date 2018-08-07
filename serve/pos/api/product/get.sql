--params(id integer)--

select * from product where ({{id}} is null or id = {{id}}) order by id

