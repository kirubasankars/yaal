--($header)--

SELECT {{$header}}::json as json, '$json' as "$type"
