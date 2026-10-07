-- Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
-- Use of this source code is governed by a MIT style
-- license that can be found in the LICENSE file.

--($args.pairs blob)--

select
    u.user_id as id,
    u.user_name as name
from users u
where optional_groups_or({{$args.pairs}}, u.user_id in ({{ids}}))
order by u.user_id
