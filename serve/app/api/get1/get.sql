--(id integer)--

SELECT {{id}} as order_id, '5/5/2018' as order_date, 1 as item_id, 1 as qty, 1 as product_id, 'Apple' as product_name
union all
SELECT {{id}} as order_id, '5/5/2018' as order_date, 2 as item_id, 1 as qty, 2 as product_id, 'Orange' as product_name
union all
SELECT {{id}} as order_id, '5/5/2018' as order_date, 3 as item_id, 1 as qty, 3 as product_id, 'Pineapple' as product_name
