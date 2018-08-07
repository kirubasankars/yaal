--params(name, $last_inserted_id integer)--

insert into product(name, active) values({{name}}, 1)

--query()--

select  rowid,* from product where rowid = {{$last_inserted_id}} 