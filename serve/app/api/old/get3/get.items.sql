--($parent.id integer)--

select * from (SELECT 1 as item_id, 1 as qty, 1 as product_id, 'Apple' as product_name, 1 as order_id
union all
SELECT 2 as item_id, 1 as qty, 2 as product_id, 'Orange' as product_name, 1 as order_id
union all
SELECT 3 as item_id, 1 as qty, 3 as product_id, 'Pineapple' as product_name, 2 as order_id) 
where {{$parent.id}} is null or order_id = {{$parent.id}}