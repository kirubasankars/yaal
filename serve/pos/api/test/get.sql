--($params.namespace string, $params.path string)--

--query()(sqlite3)--

select {{$params.namespace}} || "/" || {{$params.path}} as path;