-- Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
-- Use of this source code is governed by a MIT style
-- license that can be found in the LICENSE file.

--($args.apply bool, $args.id integer)--

select
    u.user_id as id,
    u.user_name as name
from users u
where u.user_id > 0
  and optional_when({{$args.apply}}, u.user_id = {{$args.id}})
order by u.user_id
