import csv
import os
import re
import subprocess
import time
import uuid
import logging
from datetime import datetime
from typing import Dict, List

# ---- Configuration ----
FIELD_DELIMITER = "|"
MULTI_VAL_DELIMITER = "~"
LOG_DIR = "logs"
LINUX_PROCESS_SCRIPT = "./send_fix_message.sh"
CURRENT_LOG_FILE = os.path.join(LOG_DIR, "Current")

# ---- Setup Execution Context ----
execution_id = datetime.now().strftime("%y%m%d_%H%M%S")
result_file = f"test_result_{execution_id}.csv"
summary_file = f"test_summary_{execution_id}.csv"
log_file = f"fix_test_run_{execution_id}.log"

# ---- Logging ----
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
    tags["52"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return build_fix(tags)

def validate_tags(expected: Dict[str, str], actual: Dict[str, str], testcase_id: str, tag11: str) -> (bool, List[str]):
    result = True
    messages = []
    for tag, exp_val in expected.items():
        act_val = actual.get(tag)
        if exp_val == "":
            if tag in actual:
                messages.append(f"FAIL: Tag {tag} was expected to be deleted but found {act_val} [TC: {testcase_id}, 11={tag11}]")
                result = False
            else:
                messages.append(f"PASS: Tag {tag} correctly deleted [TC: {testcase_id}, 11={tag11}]")
        elif act_val != exp_val:
            messages.append(f"FAIL: Tag {tag} value mismatch. Expected: {exp_val}, Actual: {act_val} [TC: {testcase_id}, 11={tag11}]")
            result = False
        else:
            messages.append(f"PASS: Tag {tag} matched with value {exp_val} [TC: {testcase_id}, 11={tag11}]")
    return result, messages

def send_fix_message(fix_msg: str, tag11: str, msg_type: str) -> str:
    fix_msg_sod = fix_msg.replace(FIELD_DELIMITER, '\x01')
    subprocess.run([LINUX_PROCESS_SCRIPT, fix_msg_sod])
    for _ in range(5):
        time.sleep(0.3)
        with open(CURRENT_LOG_FILE, "r") as f:
            for line in f:
                if f"11={tag11}" in line and f"35={msg_type}" in line:
                    return line.strip()
    return ""

def expand_test_cases(row: Dict[str, str]) -> List[Dict[str, str]]:
    update_parts = row["TagsToUpdate"].split(FIELD_DELIMITER)
    validate_parts = row["TagsToValidate"].split(FIELD_DELIMITER)

    update_dict = {}
    multi_tag = None
    multi_values = []

    for part in update_parts:
        if "~" in part:
            tag, values = part.split("=")
            multi_tag = tag
            multi_values = values.split(MULTI_VAL_DELIMITER)
        else:
            if "=" in part:
                tag, value = part.split("=")
                update_dict[tag] = value

    expanded_cases = []
    for idx, val in enumerate(multi_values):
        update = update_dict.copy()
        update[multi_tag] = val

        validate = {}
        for part in validate_parts:
            if "=" in part:
                tag, values = part.split("=")
                val_list = values.split(MULTI_VAL_DELIMITER)
                if tag == multi_tag and idx < len(val_list):
                    validate[tag] = val_list[idx]
                elif "~" in values:
                    continue
                else:
                    validate[tag] = values
        expanded_cases.append({
            "UseCaseID": row["UseCaseID"],
            "TestCaseID": row["TestCaseID"],
            "BaseMessage": row["BaseMessage"],
            "TagsToUpdate": update,
            "TagsToValidate": validate
        })
    return expanded_cases

# ---- Main Execution ----
def run_test(input_file: str):
    logging.info(f"Input File: {input_file}")
    logging.info(f"Result File: {result_file}")
    logging.info(f"Summary File: {summary_file}")
    logging.info(f"Log File: {log_file}")

    total = 0
    passed = 0
    failed = 0
    result_rows = []

    with open(input_file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cases = expand_test_cases(row)
            for case in cases:
                total += 1
                usecase_id = case["UseCaseID"]
                test_case_id = case["TestCaseID"]
                tag11 = f"{test_case_id}_{uuid.uuid4().hex[:4]}"
                base_msg = case["BaseMessage"]
                updated_fix = update_fix(base_msg, {**case["TagsToUpdate"], "11": tag11})
                msg_type = parse_fix(updated_fix).get("35", "D")

                sent_msg = updated_fix
                received_msg = send_fix_message(updated_fix, tag11, msg_type)
                received_tags = parse_fix(received_msg, delimiter='\x01') if received_msg else {}

                is_pass, messages = validate_tags(case["TagsToValidate"], received_tags, test_case_id, tag11)
                if is_pass:
                    passed += 1
                    logging.info(f"{tag11} [PASS]")
                else:
                    failed += 1
                    logging.error(f"{tag11} [FAIL] Reason(s): {' | '.join(messages)}")

                result_rows.append({
                    "UseCaseID": usecase_id,
                    "TestCaseID": test_case_id,
                    "ExecutionID": tag11,
                    "ValidationResult": "PASS" if is_pass else "FAIL",
                    "ValidationDetails": " | ".join(messages),
                    "SentFixMessage": sent_msg,
                    "ReceivedFixMessage": received_msg
                })

    # Write results
    with open(result_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=result_rows[0].keys())
        writer.writeheader()
        writer.writerows(result_rows)

    # Aggregate per UseCaseID
    summary_data = {}
    for row in result_rows:
        ucid = row["UseCaseID"]
        if ucid not in summary_data:
            summary_data[ucid] = {"Total": 0, "Passed": 0, "Failed": 0}
        summary_data[ucid]["Total"] += 1
        if row["ValidationResult"] == "PASS":
            summary_data[ucid]["Passed"] += 1
        else:
            summary_data[ucid]["Failed"] += 1

    # Write summary file
    with open(summary_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["UseCaseID", "Total", "Passed", "Failed"])
        for ucid, stats in summary_data.items():
            writer.writerow([ucid, stats["Total"], stats["Passed"], stats["Failed"]])


# ---- Entry Point ----
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_test_runner.py <input_csv_file>")
        sys.exit(1)
    run_test(sys.argv[1])
