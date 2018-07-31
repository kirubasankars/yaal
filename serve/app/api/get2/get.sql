--(id)--

select * from (
SELECT 1 as order_id, '5/5/2018' as order_date, 1 as item_id, 1 as qty, 1 as product_id, 'Apple' as product_name
union all
SELECT 1 as order_id, '5/5/2018' as order_date, 2 as item_id, 1 as qty, 2 as product_id, 'Orange' as product_name
union all
SELECT 2 as order_id, '5/5/2018' as order_date, 3 as item_id, 1 as qty, 3 as product_id, 'A' as product_name
) a where order_id = {{id}}
