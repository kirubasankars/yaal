--params($parent.page integer)--

SELECT (CASE WHEN total_pages = CAST(total_pages as INTEGER) THEN CAST(total_pages as INTEGER) ELSE CAST(total_pages as INTEGER) + 1 END) AS total_pages, current_page 

FROM

(SELECT COUNT(*) / 10.00 as total_pages, {{$parent.page}} as current_page FROM Customer) T
