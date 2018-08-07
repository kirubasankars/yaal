--params($parent.page integer)--

select (CASE when total_pages = CAST(total_pages as Integer) then 
            CAST(total_pages as Integer)
       else 
            CAST(total_pages as Integer) + 1
       end) as total_pages, current_page
from (select {{$parent.page}} as current_page, count(*) / 10.00 as total_pages from customer) t