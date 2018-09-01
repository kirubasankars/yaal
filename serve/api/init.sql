--($params.path)--

--query(sqlite3)--

select 1 as "$error", "sas" as message where {{$params.path}} = 'film'