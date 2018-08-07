--params($parent.page integer, $parent.total_pages integer)--

select count(*) / 10.00 as "$parent.total_pages", 1 as "$params" from Customer

--query()--

SELECT (CASE WHEN total_pages = CAST(total_pages as INTEGER) THEN CAST(total_pages as INTEGER) ELSE CAST(total_pages as INTEGER) + 1 END) AS total_pages, current_page 

FROM

(SELECT {{$parent.total_pages}} as total_pages, {{$parent.page}} as current_page) T
