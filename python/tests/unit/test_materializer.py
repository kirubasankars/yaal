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
from yaal_materializer import map_into, map_list, map_object, throw_if_errors


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
        shaped = {"id": 1, "name": "a", "extra": "ignored"}
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
        shaped = {"id": 7, "name": "x"}
        user = Yaal.materialize(UserRowDto, shaped)
        self.assertEqual(user.id, 7)

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


if __name__ == "__main__":
    unittest.main()
