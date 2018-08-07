--params($parent.page integer)--

select count(*)/10.00 as total_pages, {{$parent.page}} as current_page from product 

