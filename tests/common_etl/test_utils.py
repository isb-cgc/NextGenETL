import unittest
from common_etl.utils import *

example_dict_1 = {
    "parent_key_int": 1,
    "parent_key_list": [
        {
            "list_child_key_1": None,
            "list_child_key_2": 22,
            "list_child_key_3": 33,
        },
        {
            "list_child_key_1": True,
            "list_child_key_2": 23,
            "list_child_key_3": 34,
        },
        {
            "list_child_key_1": True,
            "list_child_key_2": 24,
            "list_child_key_3": 35,
        },

    ],
    "parent_key_str": "test str",
    "parent_key_dict": {
        "dict_child_key_1": 11,
        "dict_child_key_2": 22,
        "dict_child_key_3": 33,
    }
}

example_dict_2 = {
    "parent_key_int": 2,
    "parent_key_list": [
        {
            "list_child_key_1": True,
            "list_child_key_2": 32,
            "list_child_key_3": 43.1,
        },
        {
            "list_child_key_1": False,
            "list_child_key_2": 33,
            "list_child_key_3": 44.1,
        },
        {
            "list_child_key_1": True,
            "list_child_key_2": 44,
            "list_child_key_3": 55.1,
        },

    ],
    "parent_array": [9, 8, 7],
    "parent_key_str": "test str 2",
    "parent_key_dict": {
        "dict_child_key_1": 21,
        "dict_child_key_2": 32,
        "dict_child_key_3": 43,
    }
}

example_list = [example_dict_1, example_dict_2]


class TestUtils(unittest.TestCase):

    def test_resolve_type_conflict(self):
        print("Testing resolve_type_conflict()")
        # datetime: DATE, TIME, TIMESTAMP
        # numbers: INT64, FLOAT64, NUMERIC
        # other: STRING, BOOL, ARRAY, RECORD

        type_conflict_tuples = [
            ({}, "STRING"),
            ({"FLOAT64", "INT64", "NUMERIC"}, "FLOAT64"),
            ({"INT64", "NUMERIC"}, "NUMERIC"),
            ({"STRING", "BOOL"}, "STRING")
        ]

        self.assertEqual(resolve_type_conflict("", {}), "STRING")
        self.assertEqual(resolve_type_conflict("", {"FLOAT64", "INT64", "NUMERIC"}), "FLOAT64")
        self.assertEqual(resolve_type_conflict("", {"INT64", "NUMERIC"}), "NUMERIC")
        self.assertEqual(resolve_type_conflict("", {"STRING", "BOOL"}), "STRING")

        for type_conflict_tuple in type_conflict_tuples:
            resolved_type = resolve_type_conflict("", type_conflict_tuple[0])

            assert resolved_type == type_conflict_tuple[1], \
                f"Expected {type_conflict_tuple[0]} to resolve to: {type_conflict_tuple[1]}, actual: {resolved_type}"

        with self.assertRaises(TypeError):
            resolve_type_conflict("", {"BOOL", "INT64"})
            resolve_type_conflict("", {"STRING", "ARRAY"})
            resolve_type_conflict("", {"STRING", "RECORD"})
            resolve_type_conflict("", {"ARRAY", "RECORD"})

    def test_check_value_type(self):
        print("Testing check_value_type()")
        value_type_dict = {
            "000": "STRING",
            "0.0": "INT64",
            "100": "INT64",
            "-5403": "INT64",
            "-10001.0": "INT64",
            "0.001": "FLOAT64",
            "NaN": "FLOAT64",
            "nan": "FLOAT64",
            "2.01803E+13": "FLOAT64",
            "inf": "FLOAT64",
            "-inf": "FLOAT64",
            "Infinity": "FLOAT64",
            "Hi": "STRING",
            "0.1.1": "STRING",
            "1.1.1": "STRING",
            "111-222": "STRING",
            "Hello": "STRING",
            "2000-12-31": "DATE",
            "2000-1-1": "DATE",
            "2000-01-01": "DATE",
            "9:03:22.0001": "TIME",
            "09:03:22": "TIME",
            "9:3:22": "TIME",
            "2019-05-01T13:44:50.898263-05:00": "TIMESTAMP",
            "2019-05-01 13:44:50.898263-05:00": "TIMESTAMP",
            "2019-05-01T13:44:50.898263": "TIMESTAMP",
            "2019-05-01 13:44:50.898263": "TIMESTAMP",
            "2019-5-1T13:44:50.898263": "TIMESTAMP",
            "True": "BOOL",
            "False": "BOOL",
            "yes": "BOOL",
            "no": "BOOL",
            "1": "BOOL",
            "0": "BOOL",
            "true": "BOOL",
            "false": "BOOL"
        }

        for value, expected_type in value_type_dict.items():
            actual_type = check_value_type(value)

            assert expected_type == actual_type, \
                "Type mismatch for {}: expected {}, actual {}".format(value, expected_type, actual_type)

        print("Types checked successfully!")

