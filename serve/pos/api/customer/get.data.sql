--($parent.page integer)--

SELECT 
    c.*,
    e.FirstName AS SupportRep_FirstName,
    e.LastName AS SupportRep_LastName
FROM 
    Customers c 
JOIN 
    Employees e ON c.SupportRepId = e.EmployeeId 
ORDER BY
    c.CustomerId
LIMIT ({{$parent.page}} - 1) * 10, 10
    
