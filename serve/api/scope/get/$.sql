--(id integer)--

select * from (select 'vetri' as name, 1 as id
union
select 'dev' as name, 2 as id) a where id = {{id}} 
