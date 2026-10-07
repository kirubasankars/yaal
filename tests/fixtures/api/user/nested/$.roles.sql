-- Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
-- Use of this source code is governed by a MIT style
-- license that can be found in the LICENSE file.

--($args.id integer)--

select
    ur.user_id,
    r.role_id,
    r.role_name
from user_roles ur
inner join roles r on r.role_id = ur.role_id
where r.active = 1
  and optional(ur.user_id = {{$args.id}})
order by ur.user_id, r.role_id
