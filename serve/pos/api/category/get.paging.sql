--($query.page integer)--

select 1 as "$error", 'page can''t be less then 1.' as "message" where {{$query.page}} < 1

--query()()--

SELECT {{$query.page}} as current_page, ceil(count(*) / 10::float) as total_pages FROM category
