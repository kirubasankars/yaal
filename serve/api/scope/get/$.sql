--($query json)--

select json_extract(json({{$query}}), '$.a') as query