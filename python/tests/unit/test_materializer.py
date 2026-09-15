# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

import dataclasses
import os
import sqlite3
import tempfile
import unittest
from enum import Enum
from typing import List, Optional

from yaal import Yaal
from yaal_errors import YaalQueryError
from yaal_materializer import map_into, map_list, map_object, throw_if_errors, yaal_ignore


@dataclasses.dataclass
class RoleDto:
    id: int = 0
    name: str = ""


@dataclasses.dataclass
class UserDto:
    id: int = 0
    name: str = ""
    roles: Optional[List[RoleDto]] = None


@dataclasses.dataclass
class UserRowDto:
    id: int = 0
    name: str = ""
    active: int = 0


@dataclasses.dataclass
class SimpleUserDto:
    id: int = 0
    name: str = ""


@dataclasses.dataclass
class UserWithIgnoredRolesDto:
    id: int = 0
    name: str = ""
    display_label: Optional[str] = yaal_ignore(default=None)
    roles: Optional[List[RoleDto]] = yaal_ignore(default=None)


@dataclasses.dataclass
class PagingDto:
    page: int = 0
    page_size: int = 0
    total_count: int = 0


@dataclasses.dataclass
class PageDto:
    paging: Optional[PagingDto] = None
    data: Optional[List[UserDto]] = None


@dataclasses.dataclass
class ProfileDto:
    name: str = ""
    bio: str = ""


@dataclasses.dataclass
class UserWithProfileDto:
    id: int = 0
    profile: Optional[ProfileDto] = None


@dataclasses.dataclass
class AppSliceDto:
    id: int = 0
    name: str = ""


@dataclasses.dataclass
class FlagsSliceDto:
    user_id: int = 0
    vip: int = 0


@dataclasses.dataclass
class CombineDto:
    app: Optional[AppSliceDto] = None
    flags: Optional[FlagsSliceDto] = None


class StatusEnum(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclasses.dataclass
class ConversionDto:
    status: StatusEnum = StatusEnum.ACTIVE
    count: int = 0


class TestObjectMaterializer(unittest.TestCase):

    def test_map_scalar_and_nested(self):
        shaped = {
            "id": 1,
            "name": "admin",
            "roles": [{"id": 10, "name": "Administrator"}],
        }
        user = map_object(UserDto, shaped)
        self.assertEqual(user.id, 1)
        self.assertEqual(user.roles[0].name, "Administrator")

    def test_map_ignores_extra_keys(self):
        shaped = {"id": 1, "name": "a", "active": 1, "extra": "ignored"}
        user = map_object(UserRowDto, shaped)
        self.assertEqual(user.id, 1)

    def test_map_list_empty(self):
        self.assertEqual(map_list(UserRowDto, None), [])
        self.assertEqual(map_list(UserRowDto, []), [])

    def test_map_into_reuses_list(self):
        user = UserDto(id=42, name="x", roles=[RoleDto(id=99, name="old")])
        shaped = {"id": 1, "roles": [{"id": 10, "name": "new"}]}
        map_into(shaped, user)
        self.assertEqual(len(user.roles), 1)
        self.assertEqual(user.roles[0].name, "new")

    def test_map_into_leaves_missing_properties(self):
        user = UserDto(id=42, name="unchanged")
        map_into({"id": 1}, user)
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, "unchanged")

    def test_throw_if_errors(self):
        shaped = {"errors": [{"message": "bad"}]}
        with self.assertRaises(YaalQueryError) as ctx:
            throw_if_errors(shaped)
        self.assertEqual(ctx.exception.errors[0]["message"], "bad")

    def test_throw_if_errors_single_dict(self):
        shaped = {"errors": {"message": "one"}}
        with self.assertRaises(YaalQueryError):
            throw_if_errors(shaped)

    def test_map_null_raises(self):
        with self.assertRaises(TypeError):
            map_object(UserDto, None)

    def test_map_list_scalar_raises(self):
        with self.assertRaises(TypeError):
            map_list(UserRowDto, "not-a-list")

    def test_enum_conversion(self):
        dto = map_object(ConversionDto, {"status": StatusEnum.ACTIVE, "count": 3})
        self.assertEqual(dto.status, StatusEnum.ACTIVE)
        self.assertEqual(dto.count, 3)

    def test_materialize_static_helpers(self):
        shaped = {"id": 7, "name": "x", "active": 1}
        user = Yaal.materialize(UserRowDto, shaped)
        self.assertEqual(user.id, 7)

    def test_map_strict_missing_field_raises(self):
        shaped = {"id": 1, "name": "admin"}
        with self.assertRaises(ValueError) as ctx:
            map_object(UserDto, shaped)
        self.assertIn("roles", str(ctx.exception))

    def test_map_yaal_ignore_skips_missing_field(self):
        shaped = {"id": 1, "name": "admin"}
        user = map_object(UserWithIgnoredRolesDto, shaped)
        self.assertEqual(user.id, 1)
        self.assertIsNone(user.display_label)

    def test_map_strict_nested_role_missing_name_raises(self):
        shaped = {
            "id": 1,
            "name": "admin",
            "roles": [{"id": 10}],
        }
        with self.assertRaises(ValueError) as ctx:
            map_object(UserDto, shaped)
        self.assertIn("name", str(ctx.exception).lower())

    def test_map_strict_empty_roles_list_succeeds(self):
        shaped = {"id": 1, "name": "admin", "roles": []}
        user = map_object(UserDto, shaped)
        self.assertEqual(user.roles, [])

    def test_map_strict_nested_profile_missing_field_raises(self):
        shaped = {"id": 1, "profile": {"name": "admin"}}
        with self.assertRaises(ValueError) as ctx:
            map_object(UserWithProfileDto, shaped)
        self.assertIn("bio", str(ctx.exception).lower())

    def test_map_strict_full_nested_profile_succeeds(self):
        shaped = {"id": 1, "profile": {"name": "admin", "bio": "engineer"}}
        user = map_object(UserWithProfileDto, shaped)
        self.assertEqual(user.profile.name, "admin")
        self.assertEqual(user.profile.bio, "engineer")

    def test_map_into_lenient_nested_skips_missing_role_field_on_new_elements(self):
        user = UserDto(id=42, name="unchanged", roles=[RoleDto(id=99, name="replaced")])
        shaped = {"id": 1, "roles": [{"id": 10}]}
        map_into(shaped, user)
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, "unchanged")
        self.assertEqual(len(user.roles), 1)
        self.assertEqual(user.roles[0].id, 10)
        self.assertEqual(user.roles[0].name, "")

    def test_map_into_lenient_reuses_existing_nested_object(self):
        profile = ProfileDto(bio="keep-me")
        user = UserWithProfileDto(id=42, profile=profile)
        map_into({"id": 1, "profile": {"name": "admin"}}, user)
        self.assertIs(user.profile, profile)
        self.assertEqual(user.profile.name, "admin")
        self.assertEqual(user.profile.bio, "keep-me")

    def test_materialize_error_dict_raises(self):
        with self.assertRaises(YaalQueryError):
            Yaal.materialize(UserRowDto, {"errors": [{"message": "fail"}]})


class TestMaterializerIntegration(unittest.TestCase):

    def setUp(self):
        repo_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..")
        )
        self.fixture_api = os.path.join(repo_root, "tests", "fixtures", "api")
        schema = os.path.join(repo_root, "docker", "sqlite", "schema.sql")
        flags_schema = os.path.join(repo_root, "docker", "sqlite", "flags_schema.sql")

        self.db_path = tempfile.mktemp(prefix="yaal-py-poco-")
        self.flags_path = tempfile.mktemp(prefix="yaal-py-poco-flags-")
        conn = sqlite3.connect(self.db_path)
        conn.executescript(open(schema).read())
        conn.close()
        conn = sqlite3.connect(self.flags_path)
        conn.executescript(open(flags_schema).read())
        conn.close()

        self.yaal = Yaal(self.fixture_api, debug=True)
        self.yaal.setup_data_provider("db", "sqlite3:///" + self.db_path)
        self.yaal.setup_data_provider("flags", "sqlite3:///" + self.flags_path)

    def tearDown(self):
        for path in (self.db_path, self.flags_path):
            try:
                os.remove(path)
            except OSError:
                pass

    def test_query_typed_user_get(self):
        user = self.yaal.query_typed("user/get", UserDto, args={"id": 1})
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, "admin")
        self.assertEqual(len(user.roles), 2)

    def test_query_list_user_list(self):
        users = self.yaal.query_list("user/list", UserRowDto, args={"active": 1})
        self.assertTrue(len(users) >= 1)
        self.assertTrue(all(u.active == 1 for u in users))

    def test_query_into_hydrates(self):
        user = UserDto(name="placeholder")
        same = self.yaal.query_into("user/get", user, args={"id": 1})
        self.assertIs(same, user)
        self.assertEqual(user.id, 1)

    def test_query_list_soft_errors(self):
        with self.assertRaises(YaalQueryError):
            self.yaal.query_list("user/list", UserRowDto, args={"sort": "nope"})

    def test_query_into_rejects_array_descriptor(self):
        row = UserRowDto()
        with self.assertRaises(TypeError):
            self.yaal.query_into("user/list", row, args={"active": 1})

    def test_query_list_empty_filter(self):
        users = self.yaal.query_list("user/list", UserRowDto, args={"active": 0})
        self.assertEqual(users, [])

    def test_query_list_user_simple_maps_lowercase_aliases(self):
        users = self.yaal.query_list("user/simple", SimpleUserDto)
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, 1)
        self.assertEqual(users[0].name, "kiruba")

    def test_query_typed_user_get_nested_roles(self):
        user = self.yaal.query_typed("user/get", UserDto, args={"id": 1})
        self.assertEqual(user.roles[0].id, 1)
        self.assertEqual(user.roles[0].name, "Administrator")
        self.assertEqual(user.roles[1].id, 2)
        self.assertEqual(user.roles[1].name, "User")

    def test_query_typed_user_nested_matches_get_roles(self):
        from_join = self.yaal.query_typed("user/get", UserDto, args={"id": 1})
        from_child = self.yaal.query_typed("user/nested", UserDto, args={"id": 1})
        self.assertEqual(from_child.id, from_join.id)
        self.assertEqual(from_child.name, from_join.name)
        self.assertEqual(len(from_child.roles), len(from_join.roles))
        self.assertEqual(
            [r.id for r in from_child.roles],
            [r.id for r in from_join.roles],
        )

    def test_query_typed_user_page_deep_nest(self):
        page = self.yaal.query_typed(
            "user/page", PageDto, args={"page": 1, "page_size": 1}
        )
        self.assertEqual(page.paging.page, 1)
        self.assertEqual(page.paging.page_size, 1)
        self.assertEqual(page.paging.total_count, 2)
        self.assertEqual(len(page.data), 1)
        self.assertEqual(page.data[0].id, 1)
        self.assertEqual(page.data[0].name, "admin")
        self.assertEqual(len(page.data[0].roles), 2)
        self.assertEqual(page.data[0].roles[0].name, "Administrator")

    def test_query_typed_user_combine_multi_branch(self):
        combo = self.yaal.query_typed("user/combine", CombineDto, args={"id": 1})
        self.assertEqual(combo.app.id, 1)
        self.assertEqual(combo.app.name, "admin")
        self.assertEqual(combo.flags.user_id, 1)
        self.assertEqual(combo.flags.vip, 1)

    def test_query_into_user_get_hydrates_nested_roles(self):
        user = UserDto(name="placeholder", roles=[RoleDto(id=99, name="stale")])
        same = self.yaal.query_into("user/get", user, args={"id": 1})
        self.assertIs(same, user)
        self.assertEqual(len(user.roles), 2)
        self.assertEqual(user.roles[0].name, "Administrator")


if __name__ == "__main__":
    unittest.main()
