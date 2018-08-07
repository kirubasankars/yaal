--params(customer_id integer, order_date string)--

insert into sales_order (customer_id, order_date)
 values({{customer_id}}, {{order_date}});