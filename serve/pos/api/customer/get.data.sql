--($parent.page integer)--

SELECT 
    c.*
FROM 
    Customer c 

ORDER BY
    c.Id
LIMIT ({{$parent.page}} - 1) * 10, {{pageSize}}
    
