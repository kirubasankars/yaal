-- Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
-- Use of this source code is governed by a MIT style
-- license that can be found in the LICENSE file.

--($args.id integer)--

select
    u.user_id,
    u.user_name
from users u
where u.active = 1
  and optional(u.user_id = {{$args.id}})
order by u.user_id
