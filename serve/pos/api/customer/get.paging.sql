--params($parent.page integer)--

SELECT {{$parent.page}} as current_page, COUNT(*) / 10.00 as total_pages FROM Customer