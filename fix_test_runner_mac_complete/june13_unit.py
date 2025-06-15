import unittest
from datetime import datetime
from fix_test_runner_base import (
    parse_fix, build_fix, update_fix, validate_tags,
    expand_test_cases, resolve_placeholders_in_dict
)

class TestFixRunner(unittest.TestCase):

    def test_parse_fix(self):
        msg = "35=D|49=BUY|56=SELL"
        self.assertEqual(parse_fix(msg), {'35': 'D', '49': 'BUY', '56': 'SELL'})

    def test_build_fix(self):
        tags = {'35': 'D', '49': 'BUY', '56': 'SELL'}
        self.assertEqual(build_fix(tags), "35=D|49=BUY|56=SELL")

    def test_update_and_delete_tags(self):
        base = "35=D|49=X|56=Y|1001=DELME"
        updated = update_fix(base, {'1001': ''})
        self.assertNotIn('1001=DELME', updated)

    def test_tag52_utc_format(self):
        base = "35=D|49=X|56=Y"
        updated = update_fix(base, {})
        tags = parse_fix(updated)
        self.assertIn("52", tags)
        self.assertTrue(tags["52"].endswith("Z"))

    def test_tag11_auto_and_manual(self):
        base = "35=D|49=BUY|56=SELL"
        updated = update_fix(base, {"11": "CUSTOM_11"})
        tags = parse_fix(updated)
        self.assertEqual(tags["11"], "CUSTOM_11")

    def test_validate_tags_success(self):
        actual = {"49": "BUY", "56": "SELL"}
        expected = {"49": "BUY", "56": "SELL"}
        result, _ = validate_tags(expected, actual, "TC1", "11")
        self.assertTrue(result)

    def test_validate_tags_failure(self):
        actual = {"49": "BUY", "56": "X"}
        expected = {"49": "BUY", "56": "SELL"}
        result, _ = validate_tags(expected, actual, "TC1", "11")
        self.assertFalse(result)

    def test_validate_tags_regex_match(self):
        actual = {"1001": "ABC"}
        expected = {"1001": "^ABC$"}
        result, _ = validate_tags(expected, actual, "TC1", "11")
        self.assertTrue(result)

    def test_validate_tags_deleted(self):
        actual = {"49": "BUY"}
        expected = {"56": ""}
        result, msgs = validate_tags(expected, actual, "TC1", "11")
        self.assertTrue(result)

    def test_multivalue_expansion(self):
        row = {
            "UseCaseID": "UC1",
            "TestCaseID": "TC1",
            "BaseMessage": "8=FIX.4.2|35=D|49=X",
            "TagsToUpdate": "1001=A~B~C|35=D",
            "TagsToValidate": "1001=A~B~C"
        }
        cases = expand_test_cases(row)
        self.assertEqual(len(cases), 3)

    def test_expand_dfg_split(self):
        row = {
            "UseCaseID": "UCX",
            "TestCaseID": "TCX",
            "BaseMessage": "8=FIX.4.2|35=D|49=X",
            "TagsToUpdate": "1001=A~B|35=D~F~G",
            "TagsToValidate": "1001=A~B"
        }
        cases = expand_test_cases(row)
        types = [c["TagsToUpdate"]["35"] for c in cases]
        self.assertIn("D", types)
        self.assertIn("F", types)
        self.assertIn("G", types)
        self.assertEqual(len(cases), 4)  # D1, D2, F1, G2

    def test_expected_failure_handling(self):
        actual = {"56": "WRONG"}
        expected = {"56": "RIGHT"}
        result, msgs = validate_tags(expected, actual, "TC2", "11")
        self.assertFalse(result)

    def test_placeholder_within_test_case(self):
        row = {
            "UseCaseID": "UC01",
            "TestCaseID": "TC10",
            "BaseMessage": "35=D|49=X|56=Y|55=FOO",
            "TagsToUpdate": "65=${55}|35=D",
            "TagsToValidate": "65=FOO"
        }
        resolved = resolve_placeholders_in_dict(row["TagsToUpdate"], parse_fix(row["BaseMessage"]), {})
        self.assertEqual(resolved["65"], "FOO")

    def test_placeholder_cross_test(self):
        cross_store = {
            "TC99": {"11": "XYZ_ABC", "65": "WOW"}
        }
        row = {
            "UseCaseID": "UC02",
            "TestCaseID": "TC100",
            "BaseMessage": "35=D|49=A|56=B",
            "TagsToUpdate": "41=${TC99.11}|54=${TC99.65}|35=D",
            "TagsToValidate": "41=XYZ_ABC|54=WOW"
        }
        resolved = resolve_placeholders_in_dict(row["TagsToUpdate"], {}, cross_store)
        self.assertEqual(resolved["41"], "XYZ_ABC")
        self.assertEqual(resolved["54"], "WOW")

    def test_multi_tag_assignment(self):
        row = {
            "UseCaseID": "UC05",
            "TestCaseID": "TC105",
            "BaseMessage": "35=D|49=A|56=B",
            "TagsToUpdate": "[60~61~62]=9|35=D",
            "TagsToValidate": "60=9|61=9|62=9"
        }
        expanded = resolve_placeholders_in_dict(row["TagsToUpdate"], {}, {})
        self.assertEqual(expanded["60"], "9")
        self.assertEqual(expanded["61"], "9")
        self.assertEqual(expanded["62"], "9")

    def test_empty_tag_removal(self):
        base = "35=D|49=X|56=Y|1005=DEL"
        updated = update_fix(base, {"1005": ""})
        self.assertNotIn("1005=DEL", updated)

    def test_repetitive_values(self):
        tags = {"1": "X", "2": "X", "3": "X"}
        self.assertTrue(all(v == "X" for v in tags.values()))

    def test_35_fill_and_others(self):
        base = "8=FIX.4.2|35=8|49=X|56=Y"
        updated = update_fix(base, {})
        tags = parse_fix(updated)
        self.assertEqual(tags["35"], "8")

    def test_log_routing_simulated(self):
        # Simulating log extraction logic
        logs = [
            "8=FIX.4.2\x0135=D\x0111=TC01_1234\x0156=BRKR\x01",
            "8=FIX.4.2\x0135=F\x0111=TC01_5678\x0156=BRKR\x01"
        ]
        match = [line for line in logs if "11=TC01_1234" in line and "35=D" in line]
        self.assertEqual(len(match), 1)

if __name__ == "__main__":
    unittest.main()