--($header, $header.host string)--

SELECT {{$header}}::json as json, {{$header.host}} as host
