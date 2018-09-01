--($query.page integer)--

--query(sqlite3)--

select 1 as "$error", "sas" as message where {{$query.page}} = 2