import unittest
from unittest import mock
from fix_test_runner_base5_8_1 import (
    parse_fix, expand_multi_tag_syntax, build_fix,
    update_fix, validate_tags, resolve_placeholders,
    expand_test_cases, send_fix_message, run_test
)

import os

class TestFixRunner(unittest.TestCase):

    # --- Unit Tests for Individual Functions ---

    def test_parse_fix_valid(self):
        fix_str = "8=FIX.4.2|35=D|49=BUY|56=SELL"
        expected = {'8': 'FIX.4.2', '35': 'D', '49': 'BUY', '56': 'SELL'}
        self.assertEqual(parse_fix(fix_str), expected)

    def test_parse_fix_invalid_missing_equal(self):
        fix_str = "8=FIX.4.2|35D|49=BUY|56=SELL"
        result = parse_fix(fix_str)
        self.assertNotIn('35D', result)

    def test_parse_fix_empty(self):
        fix_str = ""
        self.assertEqual(parse_fix(fix_str), {})

    def test_parse_fix_extra_delimiters(self):
        fix_str = "8=FIX.4.2|||35=D|49=BUY|56=SELL"
        result = parse_fix(fix_str)
        self.assertIn('8', result)
        self.assertIn('35', result)

    def test_parse_fix_special_characters(self):
        fix_str = "8=FIX@4.2|35=D|49=BUY|56=SELL"
        result = parse_fix(fix_str)
        self.assertEqual(result['8'], 'FIX@4.2')

    def test_expand_multi_tag_simple(self):
        input_text = "[54~64]=5|1001=ABC"
        expected = "54=5|64=5|1001=ABC"
        self.assertEqual(expand_multi_tag_syntax(input_text), expected)

    def test_expand_multi_tag_complex(self):
        input_text = "[54~64~74]=9|7001=XYZ"
        expected = "54=9|64=9|74=9|7001=XYZ"
        self.assertEqual(expand_multi_tag_syntax(input_text), expected)

    def test_expand_multi_tag_no_match(self):
        input_text = "54=5|1001=ABC"
        self.assertEqual(expand_multi_tag_syntax(input_text), input_text)

    def test_expand_multi_tag_empty(self):
        input_text = ""
        self.assertEqual(expand_multi_tag_syntax(input_text), "")

    def test_expand_multi_tag_deletion(self):
        input_text = "[54~64]=|1001=ABC"
        expected = "54=|64=|1001=ABC"
        self.assertEqual(expand_multi_tag_syntax(input_text), expected)

    def test_build_fix_valid(self):
        tags = {'8': 'FIX.4.2', '35': 'D'}
        expected = "8=FIX.4.2|35=D"
        self.assertEqual(build_fix(tags), expected)

    def test_build_fix_empty(self):
        self.assertEqual(build_fix({}), "")

    def test_build_fix_special_characters(self):
        tags = {'8': 'FIX@4.2'}
        expected = "8=FIX@4.2"
        self.assertEqual(build_fix(tags), expected)

    def test_update_fix_add_tag(self):
        base_fix = "8=FIX.4.2|35=D|49=BUY"
        updates = {'56': 'SELL'}
        updated_fix = update_fix(base_fix, updates)
        self.assertIn('56=SELL', updated_fix)

    def test_update_fix_update_tag(self):
        base_fix = "8=FIX.4.2|35=D|49=BUY"
        updates = {'49': 'SELL'}
        updated_fix = update_fix(base_fix, updates)
        self.assertIn('49=SELL', updated_fix)

    def test_update_fix_delete_tag(self):
        base_fix = "8=FIX.4.2|35=D|49=BUY|56=SELL"
        updates = {'49': ''}
        updated_fix = update_fix(base_fix, updates)
        self.assertNotIn('49=BUY', updated_fix)

    def test_validate_tags_success(self):
        expected_tags = {"55": "ABC"}
        actual_tags = {"55": "ABC"}
        is_pass, messages = validate_tags(expected_tags, actual_tags, "TC001", "TC001_1234")
        self.assertTrue(is_pass)

    def test_validate_tags_mismatch(self):
        expected_tags = {"55": "ABC"}
        actual_tags = {"55": "XYZ"}
        is_pass, messages = validate_tags(expected_tags, actual_tags, "TC001", "TC001_1234")
        self.assertFalse(is_pass)

    def test_validate_tags_deleted_success(self):
        expected_tags = {"55": ""}
        actual_tags = {}
        is_pass, messages = validate_tags(expected_tags, actual_tags, "TC001", "TC001_1234")
        self.assertTrue(is_pass)

    def test_validate_tags_deleted_fail(self):
        expected_tags = {"55": ""}
        actual_tags = {"55": "VAL"}
        is_pass, messages = validate_tags(expected_tags, actual_tags, "TC001", "TC001_1234")
        self.assertFalse(is_pass)

    def test_validate_tags_missing_tag(self):
        expected_tags = {"100": "FOO"}
        actual_tags = {}
        is_pass, messages = validate_tags(expected_tags, actual_tags, "TC001", "TC001_1234")
        self.assertFalse(is_pass)

    def test_validate_tags_regex_pass(self):
        expected_tags = {"100": r"\w+"}
        actual_tags = {"100": "FOO"}
        is_pass, messages = validate_tags(expected_tags, actual_tags, "TC001", "TC001_1234")
        self.assertTrue(is_pass)

    def test_validate_tags_regex_fail(self):
        expected_tags = {"100": r"\d+"}
        actual_tags = {"100": "ABC"}
        is_pass, messages = validate_tags(expected_tags, actual_tags, "TC001", "TC001_1234")
        self.assertFalse(is_pass)

    def test_resolve_placeholders_cross_test(self):
        sent_fix_messages = {"TC002": {"11": "TEST11"}}
        tags = {"41": "${TC002.11}"}
        result = resolve_placeholders(tags, "TC003", sent_fix_messages)
        self.assertEqual(result["41"], "TEST11")

    def test_resolve_placeholders_same_test(self):
        sent_fix_messages = {}
        current_sent_tags = {"55": "VALUE55"}
        tags = {"54": "${55}"}
        result = resolve_placeholders(tags, "TC003", current_sent_tags)
        self.assertEqual(result["54"], "VALUE55")

    def test_resolve_placeholders_missing(self):
        sent_fix_messages = {}
        current_sent_tags = {}
        tags = {"54": "${55}"}
        with self.assertRaises(ValueError):
            resolve_placeholders(tags, "TC003", current_sent_tags)

    def test_expand_test_cases_single(self):
        row = {"UseCaseID": "UC01", "TestCaseID": "TC001", "BaseMessage": "8=FIX.4.2|35=D", 
               "TagsToUpdate": "1001=ABC", "TagsToValidate": "1001=ABC"}
        cases, second_35 = expand_test_cases(row)
        self.assertEqual(len(cases), 1)

    def test_expand_test_cases_multi_value(self):
        row = {"UseCaseID": "UC01", "TestCaseID": "TC001", "BaseMessage": "8=FIX.4.2|35=D",
               "TagsToUpdate": "6401=FOO~BAR", "TagsToValidate": "6401=FOO~BAR"}
        cases, second_35 = expand_test_cases(row)
        self.assertEqual(len(cases), 2)

    def test_expand_test_cases_multi_tag(self):
        row = {"UseCaseID": "UC01", "TestCaseID": "TC001", "BaseMessage": "8=FIX.4.2|35=D",
               "TagsToUpdate": "[54~64]=VAL", "TagsToValidate": "[54~64]=VAL"}
        cases, second_35 = expand_test_cases(row)
        self.assertEqual(len(cases), 1)

    @mock.patch('fix_test_runner_base5_8_1.subprocess.run')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data="""UseCaseID,TestCaseID,BaseMessage,TagsToUpdate,TagsToValidate
UC01,TC001,8=FIX.4.2|35=D|49=BUY|56=SELL,1001=NEW,1001=NEW
""")
    def test_run_test_end_to_end_success(self, mock_csv_open, mock_subproc_run):
        with mock.patch('fix_test_runner_base5_8_1.send_fix_message', return_value="8=FIX.4.2\x0135=D\x0111=TC001_1234\x011001=NEW\x01"):
            run_test("dummy_input.csv")
            mock_subproc_run.assert_called()

    @mock.patch('fix_test_runner_base5_8_1.subprocess.run')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data="""UseCaseID,TestCaseID,BaseMessage,TagsToUpdate,TagsToValidate
UC02,TC002,8=FIX.4.2|49=BUY|56=SELL,1001=NEW,1001=NEW
""")
    def test_run_test_missing_tag35(self, mock_csv_open, mock_subproc_run):
        with mock.patch('fix_test_runner_base5_8_1.send_fix_message', return_value=""):
            run_test("dummy_input.csv")
            self.assertTrue(mock_subproc_run.called)

    @mock.patch('fix_test_runner_base5_8_1.subprocess.run')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data="""UseCaseID,TestCaseID,BaseMessage,TagsToUpdate,TagsToValidate
UC03,TC003,8=FIX.4.2|35=D|49=BUY|56=SELL|65=FOO,55=${65},55=FOO
""")
    def test_run_test_placeholder_same_test(self, mock_csv_open, mock_subproc_run):
        with mock.patch('fix_test_runner_base5_8_1.send_fix_message', return_value="8=FIX.4.2\x0135=D\x0111=TC003_5678\x0155=FOO\x01"):
            run_test("dummy_input.csv")
            self.assertTrue(mock_subproc_run.called)

    @mock.patch('fix_test_runner_base5_8_1.subprocess.run')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data="""UseCaseID,TestCaseID,BaseMessage,TagsToUpdate,TagsToValidate
UC04,TC004,8=FIX.4.2|35=D|49=BUY|56=SELL,1001=,1001=
""")
    def test_run_test_deletion(self, mock_csv_open, mock_subproc_run):
        with mock.patch('fix_test_runner_base5_8_1.send_fix_message', return_value="8=FIX.4.2\x0135=D\x0111=TC004_4321\x01"):
            run_test("dummy_input.csv")
            self.assertTrue(mock_subproc_run.called)

    @mock.patch('fix_test_runner_base5_8_1.subprocess.run')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data="""UseCaseID,TestCaseID,BaseMessage,TagsToUpdate,TagsToValidate
UC05,TC005,8=FIX.4.2|35=D|49=BUY|56=SELL|9999=100,35=D~G|9999=100,9999=100
""")
    def test_run_test_dg_expansion(self, mock_csv_open, mock_subproc_run):
        with mock.patch('fix_test_runner_base5_8_1.send_fix_message', side_effect=[
            "8=FIX.4.2\x0135=D\x0111=TC005_1111\x019999=100\x01",
            "8=FIX.4.2\x0135=G\x0111=TC005_2222\x019999=100\x01"
        ]):
            run_test("dummy_input.csv")
            self.assertTrue(mock_subproc_run.called)

# --- End of Unit Test Pack ---

if __name__ == '__main__':
    unittest.main()