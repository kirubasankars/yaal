-- Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
-- Use of this source code is governed by a MIT style
-- license that can be found in the LICENSE file.

--($args.pairs blob)--

select * from (select 1 as id union select 2) t
where optional_groups_or({{$args.pairs}}, id = {{id}})
