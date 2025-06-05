import csv
import os
import re
import uuid
import logging
import subprocess
import time
from datetime import datetime
from typing import Dict

# ---- Configuration ----
FIELD_DELIMITER = "|"
MULTI_VAL_DELIMITER = "~"
SOH = '\x01'  # FIX standard delimiter
OUTPUT_DIR = "output"
LINUX_PROCESS_SCRIPT = "./send_fix_message.sh"
CURRENT_LOG_FILE = "./logs/Current"

WAIT_TIME_BETWEEN_RETRIES = 0.5  # seconds
MAX_RETRIES = 8  # number of retries to read response

# ---- Setup Execution Context ----
os.makedirs(OUTPUT_DIR, exist_ok=True)

execution_id = datetime.now().strftime("%y%m%d_%H%M%S")
result_file = os.path.join(OUTPUT_DIR, f"test_result_{execution_id}.csv")
summary_file = os.path.join(OUTPUT_DIR, f"test_summary_{execution_id}.csv")
log_file = os.path.join(OUTPUT_DIR, f"fix_test_run_{execution_id}.log")

# ---- Logging ----
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()

# ---- Global Storage ----
sent_fix_messages = {}  # {TestCaseID: {tag: value}}

# ---- Utility Functions ----
def expand_multi_tag_syntax(text: str) -> str:
    pattern = re.compile(r'\[([^\]]+)\]=([^|]*)')
    matches = pattern.findall(text)
    for group, value in matches:
        tags = group.split("~")
        expanded = '|'.join(f"{tag}={value}" for tag in tags)
        text = text.replace(f"[{group}]={value}", expanded)
    return text

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

def validate_tags(expected: Dict[str, str], actual: Dict[str, str], testcase_id: str, tag11: str) -> (bool, list):
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
    for attempt in range(MAX_RETRIES):
        time.sleep(WAIT_TIME_BETWEEN_RETRIES)
        with open(CURRENT_LOG_FILE, "r") as f:
            for line in f:
                if f"11={tag11}" in line and f"35={msg_type}" in line:
                    log.info(f"Received response: {line.strip().replace(SOH, FIELD_DELIMITER)}")
                    return line.strip()
    log.error(f"No response found for tag 11={tag11}")
    return ""

def resolve_placeholders(tag_dict: Dict[str, str], testcase_id: str, current_sent_tags: Dict[str, str]) -> Dict[str, str]:
    resolved = {}
    pattern = re.compile(r"\$\{([^}]+)\}")
    for tag, val in tag_dict.items():
        matches = pattern.findall(val)
        for match in matches:
            if "." in match:
                ref_tc, ref_tag = match.split(".", 1)
                if ref_tc not in sent_fix_messages or ref_tag not in sent_fix_messages[ref_tc]:
                    raise ValueError(f"Placeholder {match} not found.")
                replacement = sent_fix_messages[ref_tc][ref_tag]
            else:
                if match not in current_sent_tags:
                    raise ValueError(f"Placeholder {match} not found.")
                replacement = current_sent_tags[match]
            val = val.replace(f"${{{match}}}", replacement)
        resolved[tag] = val
    return resolved

def expand_test_cases(row: Dict[str, str]) -> (list, str):
    row["TagsToUpdate"] = expand_multi_tag_syntax(row["TagsToUpdate"])
    row["TagsToValidate"] = expand_multi_tag_syntax(row["TagsToValidate"])

    update_parts = row["TagsToUpdate"].split(FIELD_DELIMITER)
    validate_parts = row["TagsToValidate"].split(FIELD_DELIMITER)

    update_dict_fixed = {}
    multi_tag = None
    multi_values = []
    validate_multi_values = {}
    multi_35_values = None

    for part in update_parts:
        if "=" not in part:
            continue
        tag, value = part.split("=", 1)
        if tag == "35" and MULTI_VAL_DELIMITER in value:
            multi_35_values = value.split(MULTI_VAL_DELIMITER)
        elif MULTI_VAL_DELIMITER in value:
            if multi_tag and tag != multi_tag:
                raise ValueError(f"Only one multi-valued tag (besides 35) allowed! Found another: {tag}")
            multi_tag = tag
            multi_values = value.split(MULTI_VAL_DELIMITER)
        else:
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
            if multi_35_values:
                update["35"] = multi_35_values[0]
            validate = {}
            for tag, values_list in validate_multi_values.items():
                validate[tag] = values_list[min(idx, len(values_list)-1)]
            expanded_cases.append({
                "UseCaseID": row["UseCaseID"],
                "TestCaseID": row["TestCaseID"],
                "BaseMessage": row["BaseMessage"],
                "TagsToUpdate": update,
                "TagsToValidate": validate
            })
    else:
        update = update_dict_fixed.copy()
        if multi_35_values:
            update["35"] = multi_35_values[0]
        validate = {tag: vals[0] for tag, vals in validate_multi_values.items()}
        expanded_cases.append({
            "UseCaseID": row["UseCaseID"],
            "TestCaseID": row["TestCaseID"],
            "BaseMessage": row["BaseMessage"],
            "TagsToUpdate": update,
            "TagsToValidate": validate
        })

    log.info(f"Expanded test cases: {expanded_cases}")
    return expanded_cases, multi_35_values[1] if multi_35_values else None

def run_test(input_file: str):
    log.info(f"Execution started with Input File: {input_file}")
    total, passed, failed = 0, 0, 0
    result_rows = []

    with open(input_file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        all_rows = list(reader)

        for row in all_rows:
            base_cases, second_35_value = expand_test_cases(row)
            final_cases = []

            for case in base_cases:
                tags_to_update = case["TagsToUpdate"]
                if "11" in tags_to_update and tags_to_update["11"]:
                    tag11 = tags_to_update["11"]
                else:
                    tag11 = f"{case['TestCaseID']}_{uuid.uuid4().hex[:8]}"
                    tags_to_update["11"] = tag11
                case["tag11"] = tag11
                final_cases.append(case)
                if second_35_value:
                    g_case = {
                        "UseCaseID": case["UseCaseID"],
                        "TestCaseID": case["TestCaseID"],
                        "BaseMessage": case["BaseMessage"],
                        "TagsToUpdate": tags_to_update.copy(),
                        "TagsToValidate": case["TagsToValidate"].copy()
                    }
                    g_case["TagsToUpdate"]["35"] = second_35_value
                    g_case["TagsToUpdate"]["41"] = tag11
                    g_case["tag11"] = f"{case['TestCaseID']}_{uuid.uuid4().hex[:8]}"
                    g_case["TagsToUpdate"]["11"] = g_case["tag11"]
                    final_cases.append(g_case)

            for idx, case in enumerate(final_cases, start=1):
                testcase_id = case['TestCaseID']
                tag11 = case['tag11']
                print(f"[{idx}/{len(final_cases)}] UseCaseID: {case['UseCaseID']}, TestCaseID: {testcase_id}, 11: {tag11} ...", end=" ")

                try:
                    resolved_update = resolve_placeholders(case["TagsToUpdate"], testcase_id, sent_fix_messages.get(testcase_id, {}))
                except Exception as e:
                    print("❌ Placeholder Resolution Failed")
                    failed += 1
                    result_rows.append({
                        "UseCaseID": case["UseCaseID"],
                        "TestCaseID": testcase_id,
                        "ExecutionID": "N/A",
                        "ValidationResult": "FAIL",
                        "ValidationDetails": f"Placeholder resolution failed: {e}",
                        "MessageType": "MISSING",
                        "SentFixMessage": "",
                        "ReceivedFixMessage": ""
                    })
                    continue

                base_msg = case["BaseMessage"]
                updated_fix = update_fix(base_msg, resolved_update)
                fix_tags = parse_fix(updated_fix)
                sent_msg_tags = parse_fix(updated_fix)
                sent_fix_messages[testcase_id] = sent_msg_tags

                msg_type = fix_tags.get("35", "")
                if not msg_type:
                    print("❌ Mandatory tag 35 missing")
                    failed += 1
                    result_rows.append({
                        "UseCaseID": case["UseCaseID"],
                        "TestCaseID": testcase_id,
                        "ExecutionID": tag11,
                        "ValidationResult": "FAIL",
                        "ValidationDetails": "Mandatory tag 35 missing",
                        "MessageType": "MISSING",
                        "SentFixMessage": updated_fix,
                        "ReceivedFixMessage": ""
                    })
                    continue

                try:
                    resolved_validate = resolve_placeholders(case["TagsToValidate"], testcase_id, sent_msg_tags)
                except Exception as e:
                    print("❌ Validation Placeholder Resolution Failed")
                    failed += 1
                    result_rows.append({
                        "UseCaseID": case["UseCaseID"],
                        "TestCaseID": testcase_id,
                        "ExecutionID": tag11,
                        "ValidationResult": "FAIL",
                        "ValidationDetails": f"Validation placeholder resolution failed: {e}",
                        "MessageType": msg_type,
                        "SentFixMessage": updated_fix,
                        "ReceivedFixMessage": ""
                    })
                    continue

                received_msg = send_fix_message(updated_fix, tag11, msg_type)
                received_tags = parse_fix(received_msg, delimiter=SOH) if received_msg else {}

                is_pass, messages = validate_tags(resolved_validate, received_tags, testcase_id, tag11)

                # Log validation messages
                for message in messages:
                    log.info(f"[{testcase_id}] {message}")

                if is_pass:
                    passed += 1
                    print("✅ PASS")
                else:
                    failed += 1
                    print("❌ FAIL")

                result_rows.append({
                    "UseCaseID": case["UseCaseID"],
                    "TestCaseID": testcase_id,
                    "ExecutionID": tag11,
                    "ValidationResult": "PASS" if is_pass else "FAIL",
                    "ValidationDetails": " | ".join(messages),
                    "MessageType": msg_type,
                    "SentFixMessage": updated_fix,
                    "ReceivedFixMessage": received_msg.replace(SOH, FIELD_DELIMITER) if received_msg else ""
                })

    with open(result_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=result_rows[0].keys())
        writer.writeheader()
        writer.writerows(result_rows)

    summary_data = {}
    for row in result_rows:
        key = (row["UseCaseID"], row["TestCaseID"], row["MessageType"])
        if key not in summary_data:
            summary_data[key] = {"Total": 0, "Passed": 0, "Failed": 0}
        summary_data[key]["Total"] += 1
        if row["ValidationResult"] == "PASS":
            summary_data[key]["Passed"] += 1
        else:
            summary_data[key]["Failed"] += 1

    with open(summary_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["UseCaseID", "TestCaseID", "MessageType", "Total", "Passed", "Failed"])
        for (ucid, tcid, msg_type), stats in summary_data.items():
            writer.writerow([ucid, tcid, msg_type, stats["Total"], stats["Passed"], stats["Failed"]])

    print(f"\nInput File: {input_file}")
    print(f"Result File: {result_file}")
    print(f"Summary File: {summary_file}")
    print(f"Log File: {log_file}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_test_runner.py <input_csv_file>")
        sys.exit(1)
    run_test(sys.argv[1])