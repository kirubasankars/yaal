--params($parent.page integer, total_pages integer)--

SELECT 
    1 as "$params",
    (count(*) / 10::float) as total_pages
FROM 
    film

--query()(sqlite3)--

SELECT 
    1 as "$error", 
    1 as code, 
    "page can't less then 1 or more then " || {{$parent.page}} as message
WHERE 
    {{$parent.page}} < 1 or {{$parent.page}} > {{total_pages}} 

--query()(sqlite3)--

SELECT
    {{$parent.page}} as current_page,
    {{total_pages}} as total_pages,
    CASE WHEN {{$parent.page}} < {{total_pages}} THEN 
        ("http://localhost:5000/pos/api/film?page=" || ({{$parent.page}} + 1))
    ELSE
        null
    END as next_page
    