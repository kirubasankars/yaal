--($parent.page integer)--


select {{$parent.page}} as 'current_page', (case when x = cast(x as int) then 
                                                cast(x as int) 
                                            else 
                                                1 + cast(x as int)
                                            end) as 'total_pages' 

from (select count(*) / 10.0 as x from customers) a;