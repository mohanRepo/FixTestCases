import csv
import os
import re
import uuid
import logging
import subprocess
import time
from datetime import datetime
from typing import Dict, List

# ---- Configuration ----
FIELD_DELIMITER = "|"
MULTI_VAL_DELIMITER = "~"
SOH = '\x01'  # FIX standard delimiter
OUTPUT_DIR = "output"
LINUX_PROCESS_SCRIPT = "./send_fix_message.sh"
CURRENT_LOG_FILE = "./logs/Current"

# ---- Setup Execution Context ----
os.makedirs(OUTPUT_DIR, exist_ok=True)

execution_id = datetime.now().strftime("%y%m%d_%H%M%S")
result_file = os.path.join(OUTPUT_DIR, f"test_result_{execution_id}.csv")
summary_file = os.path.join(OUTPUT_DIR, f"test_summary_{execution_id}.csv")
log_file = os.path.join(OUTPUT_DIR, f"fix_test_run_{execution_id}.log")

# ---- Logging ----
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

# ---- Utility Functions ----
def parse_fix(fix_str: str, delimiter=FIELD_DELIMITER) -> Dict[str, str]:
    return dict(item.split("=", 1) for item in fix_str.split(delimiter) if "=" in item)

def build_fix(tags: Dict[str, str]) -> str:
    return FIELD_DELIMITER.join(f"{k}={v}" for k, v in tags.items())

def update_fix(base_fix: str, updates: Dict[str, str]) -> str:
    tags = parse_fix(base_fix)
    for k, v in updates.items():
        if v == "":
            tags.pop(k, None)
        else:
            tags[k] = v
    tags["52"] = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S")
    updated_fix = build_fix(tags)
    log.info(f"Updated FIX message: {updated_fix}")
    return updated_fix

def validate_tags(expected: Dict[str, str], actual: Dict[str, str], testcase_id: str, tag11: str) -> (bool, List[str]):
    result = True
    messages = []
    for tag, exp_pattern in expected.items():
        act_val = actual.get(tag)
        if exp_pattern == "":
            if tag in actual:
                messages.append(f"FAIL: Tag {tag} expected deleted but found {act_val} [TC: {testcase_id}, 11={tag11}]")
                result = False
            else:
                messages.append(f"PASS: Tag {tag} correctly deleted [TC: {testcase_id}, 11={tag11}]")
        else:
            if act_val is None or not re.fullmatch(exp_pattern, act_val):
                messages.append(f"FAIL: Tag {tag} regex mismatch. Pattern: {exp_pattern}, Actual: {act_val} [TC: {testcase_id}, 11={tag11}]")
                result = False
            else:
                messages.append(f"PASS: Tag {tag} regex match successful [{exp_pattern}] [TC: {testcase_id}, 11={tag11}]")
    return result, messages

def send_fix_message(fix_msg: str, tag11: str, msg_type: str) -> str:
    fix_msg_sod = fix_msg.replace(FIELD_DELIMITER, SOH)
    log.info(f"Sending FIX message to Linux process: {fix_msg}")
    subprocess.run([LINUX_PROCESS_SCRIPT, fix_msg_sod])
    for attempt in range(5):
        time.sleep(0.3)
        with open(CURRENT_LOG_FILE, "r") as f:
            for line in f:
                if f"11={tag11}" in line and f"35={msg_type}" in line:
                    log.info(f"Received response from Linux process: {line.strip().replace(SOH, FIELD_DELIMITER)}")
                    return line.strip()
    log.error(f"No response found for tag 11={tag11}")
    return ""

def expand_test_cases(row: Dict[str, str]) -> (List[Dict[str, str]], str):
    update_parts = row["TagsToUpdate"].split(FIELD_DELIMITER)
    validate_parts = row["TagsToValidate"].split(FIELD_DELIMITER)

    update_dict_fixed = {}
    multi_tag = None
    multi_values = []
    second_35_value = None
    validate_multi_values = {}

    for part in update_parts:
        if "~" in part:
            tag, values = part.split("=", 1)
            if tag == "35":
                values_split = values.split(MULTI_VAL_DELIMITER)
                if len(values_split) > 1:
                    multi_values = [values_split[0]]  # Only D for expansion
                    second_35_value = values_split[1]  # G or F
                multi_tag = tag
            else:
                multi_tag = tag
                multi_values = values.split(MULTI_VAL_DELIMITER)
        else:
            if "=" in part:
                tag, value = part.split("=", 1)
                update_dict_fixed[tag] = value

    for part in validate_parts:
        if "=" in part:
            tag, value = part.split("=", 1)
            validate_multi_values[tag] = value.split(MULTI_VAL_DELIMITER)

    expanded_cases = []
    if multi_tag and multi_values:
        for idx, val in enumerate(multi_values):
            update = update_dict_fixed.copy()
            update[multi_tag] = val

            validate = {}
            for tag, values_list in validate_multi_values.items():
                if idx < len(values_list):
                    validate[tag] = values_list[idx]
                else:
                    validate[tag] = values_list[-1]

            expanded_case = {
                "UseCaseID": row["UseCaseID"],
                "TestCaseID": row["TestCaseID"],
                "BaseMessage": row["BaseMessage"],
                "TagsToUpdate": update,
                "TagsToValidate": validate
            }
            expanded_cases.append(expanded_case)
    else:
        update = update_dict_fixed.copy()
        validate = {tag: vals[0] for tag, vals in validate_multi_values.items()}
        expanded_case = {
            "UseCaseID": row["UseCaseID"],
            "TestCaseID": row["TestCaseID"],
            "BaseMessage": row["BaseMessage"],
            "TagsToUpdate": update,
            "TagsToValidate": validate
        }
        expanded_cases.append(expanded_case)

    log.info(f"Expanded test cases: {expanded_cases}")
    return expanded_cases, second_35_value

# ---- Main Execution ----
def run_test(input_file: str):
    log.info(f"Execution started with Input File: {input_file}")
    log.info(f"Result File: {result_file}")
    log.info(f"Summary File: {summary_file}")
    log.info(f"Log File: {log_file}")

    total = 0
    passed = 0
    failed = 0
    result_rows = []

    with open(input_file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            log.info(f"Processing input row: {row}")
            base_cases, second_35_value = expand_test_cases(row)

            final_cases = []
            d_cases = []

            for case in base_cases:
                tags_to_update = case["TagsToUpdate"]
                if "11" in tags_to_update and tags_to_update["11"]:
                    tag11 = tags_to_update["11"]
                else:
                    tag11 = f"{case['TestCaseID']}_{uuid.uuid4().hex[:4]}"
                    tags_to_update["11"] = tag11
                case["tag11"] = tag11
                d_cases.append(case)

            for d_case in d_cases:
                final_cases.append(d_case)
                if second_35_value:
                    g_case = {
                        "UseCaseID": d_case["UseCaseID"],
                        "TestCaseID": d_case["TestCaseID"],
                        "BaseMessage": d_case["BaseMessage"],
                        "TagsToUpdate": d_case["TagsToUpdate"].copy(),
                        "TagsToValidate": d_case["TagsToValidate"].copy()
                    }
                    g_case["TagsToUpdate"]["35"] = second_35_value
                    g_case["TagsToUpdate"]["41"] = d_case["tag11"]
                    g_case["tag11"] = f"{d_case['TestCaseID']}_{uuid.uuid4().hex[:4]}"
                    g_case["TagsToUpdate"]["11"] = g_case["tag11"]
                    final_cases.append(g_case)

            for case in final_cases:
                total += 1
                usecase_id = case["UseCaseID"]
                test_case_id = case["TestCaseID"]
                tag11 = case["tag11"]
                base_msg = case["BaseMessage"]
                updated_fix = update_fix(base_msg, case["TagsToUpdate"])
                msg_type = parse_fix(updated_fix).get("35", "D")

                sent_msg = updated_fix
                received_msg = send_fix_message(updated_fix, tag11, msg_type)
                received_tags = parse_fix(received_msg, delimiter=SOH) if received_msg else {}

                is_pass, messages = validate_tags(case["TagsToValidate"], received_tags, test_case_id, tag11)
                if is_pass:
                    passed += 1
                    log.info(f"Test Case {test_case_id} [PASS]")
                else:
                    failed += 1
                    log.error(f"Test Case {test_case_id} [FAIL] Reason(s): {' | '.join(messages)}")

                result_rows.append({
                    "UseCaseID": usecase_id,
                    "TestCaseID": test_case_id,
                    "ExecutionID": tag11,
                    "ValidationResult": "PASS" if is_pass else "FAIL",
                    "ValidationDetails": " | ".join(messages),
                    "SentFixMessage": sent_msg,
                    "ReceivedFixMessage": received_msg.replace(SOH, FIELD_DELIMITER) if received_msg else ""
                })

    with open(result_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=result_rows[0].keys())
        writer.writeheader()
        writer.writerows(result_rows)

    summary_data = {}
    for row in result_rows:
        fix_msg = row["SentFixMessage"]
        fix_tags = parse_fix(fix_msg)
        if fix_tags.get("35") == "D":  # Only D messages counted
            ucid = row["UseCaseID"]
            if ucid not in summary_data:
                summary_data[ucid] = {"Total": 0, "Passed": 0, "Failed": 0}
            summary_data[ucid]["Total"] += 1
            if row["ValidationResult"] == "PASS":
                summary_data[ucid]["Passed"] += 1
            else:
                summary_data[ucid]["Failed"] += 1

    with open(summary_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["UseCaseID", "Total", "Passed", "Failed"])
        for ucid, stats in summary_data.items():
            writer.writerow([ucid, stats["Total"], stats["Passed"], stats["Failed"]])

    log.info(f"Execution finished. Total Tests: {total}, Passed: {passed}, Failed: {failed}")

    # Print file paths at the end
    print(f"Input File: {input_file}")
    print(f"Result File: {result_file}")
    print(f"Summary File: {summary_file}")
    print(f"Log File: {log_file}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_test_runner.py <input_csv_file>")
        sys.exit(1)
    run_test(sys.argv[1])
