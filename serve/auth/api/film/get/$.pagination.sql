--($query.page integer, $params.total_pages integer)--

--query()--

SELECT 
    1 as "$params",
    (count(*) / 10::float) as total_pages
FROM 
    film

--query(sqlite3)--

SELECT 
    1 as "$error", 
    1 as code, 
    "page can't less then 1 or more then " || {{$query.page}} as message
WHERE 
    {{$query.page}} < 1 or {{$query.page}} > {{$params.total_pages}} 

--query(sqlite3)--

SELECT
    {{$query.page}} as current_page,
    {{$params.total_pages}} as total_pages,
    CASE WHEN {{$query.page}} < {{$params.total_pages}} THEN 
        ("http://localhost:5000/_auth/api/film?page=" || ({{$query.page}} + 1))
    ELSE
        null
    END as next_page
    