--(pairs blob)--
select u.user_id
from users u
where optional_groups_or({{pairs}}, u.user_id = {{self.id}} and u.role_id in ({{self.ids}}...))
