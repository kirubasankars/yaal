--params(name string)--

select "Kiruba" as name, 1 as "$params";

--query()--

select {{name}} || " Sankar" as new_name