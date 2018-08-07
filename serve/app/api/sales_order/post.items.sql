--params($parent.$parent.$last_inserted_id integer, product_id integer, qty integer)--

insert into sales_order_item (order_id, product_id, qty) 
values ({{$parent.$parent.$last_inserted_id}}, {{product_id}}, {{qty}})