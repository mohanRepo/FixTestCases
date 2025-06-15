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
SOD = "\x01"
LOG_DIR = "output"
LINUX_PROCESS_SCRIPT = "./send_fix_message.sh"
CURRENT_LOG_FILE = os.path.join(LOG_DIR, "Current")
WAIT_TIME_SEC = 0.3
RETRY_COUNT = 5

execution_id = datetime.now().strftime("%y%m%d_%H%M%S")
result_out = f"{LOG_DIR}/test_result_{execution_id}.csv"
summary_out = f"{LOG_DIR}/test_summary_{execution_id}.csv"
log_file = f"{LOG_DIR}/fix_test_run_{execution_id}.log"

logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---- Utility Functions ----

def parse_fix(fix_str: str, delimiter: str = FIELD_DELIMITER) -> Dict[str, str]:
    return dict(item.split("=", 1) for item in fix_str.split(delimiter) if "=" in item)

def build_fix(tags: Dict[str, str], delimiter: str = FIELD_DELIMITER) -> str:
    return delimiter.join(f"{k}={v}" for k, v in tags.items())

def apply_tag_expansion(tag_map: Dict[str, str]) -> Dict[str, str]:
    expanded = {}
    for k, v in tag_map.items():
        if k.startswith("[") and "]" in k:
            tags = k.strip("[]").split("~")
            for t in tags:
                expanded[t] = v
        else:
            expanded[k] = v
    return expanded

def update_fix(base_fix: str, updates: Dict[str, str]) -> str:
    tags = parse_fix(base_fix)
    updates = apply_tag_expansion(updates)
    for k, v in updates.items():
        if v == "":
            tags.pop(k, None)
        else:
            tags[k] = v
    tags["52"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return build_fix(tags)

def resolve_placeholders(value: str, current_tags: Dict[str, str], sent_messages: Dict[str, Dict[str, str]]) -> str:
    pattern = r"\$\{([^}]+)\}"
    matches = re.findall(pattern, value)
    for match in matches:
        if "." in match:
            ref_tc, tag = match.split(".")
            ref_val = sent_messages.get(ref_tc, {}).get(tag)
        else:
            ref_val = current_tags.get(match)
        if ref_val is not None:
            value = value.replace("${" + match + "}", ref_val)
    return value

def validate_tags(expected: Dict[str, str], actual: Dict[str, str], testcase_id: str, tag11: str) -> (bool, List[str]):
    result = True
    messages = []
    for tag, exp_val in expected.items():
        if exp_val == "":
            if tag in actual:
                messages.append(f"FAIL: Tag {tag} was expected to be deleted but found {actual[tag]}")
                result = False
            else:
                messages.append(f"PASS: Tag {tag} correctly deleted")
        else:
            act_val = actual.get(tag, "")
            try:
                if re.fullmatch(exp_val, act_val):
                    messages.append(f"PASS: Tag {tag} matched value {act_val}")
                else:
                    messages.append(f"FAIL: Tag {tag} value mismatch. Expected: {exp_val}, Actual: {act_val}")
                    result = False
            except Exception as e:
                messages.append(f"FAIL: Invalid regex for tag {tag}: {exp_val}")
                result = False
    return result, messages

def send_fix_message(fix_msg: str, tag11: str, msg_type: str) -> str:
    subprocess.run([LINUX_PROCESS_SCRIPT, fix_msg.replace(FIELD_DELIMITER, SOD)])
    for _ in range(RETRY_COUNT):
        time.sleep(WAIT_TIME_SEC)
        try:
            with open(CURRENT_LOG_FILE, "r") as f:
                for line in f:
                    if f"11={tag11}" in line and f"35={msg_type}" in line:
                        return line.strip()
        except Exception:
            continue
    return ""

def expand_35_dfg(row: Dict[str, str]) -> List[Dict[str, str]]:
    """Handles splitting 35=D~F~G into 35=D~F and 35=D~G as two independent test cases."""
    updates = row["TagsToUpdate"]
    match = re.search(r"(35=)([^|]+)", updates)
    if not match:
        return [row]

    values = match.group(2).split(MULTI_VAL_DELIMITER)
    if "D" in values and ("F" in values or "G" in values):
        rows = []
        for x in ["F", "G"]:
            if x in values:
                new_row = row.copy()
                new_row["TagsToUpdate"] = updates.replace(f"35={'~'.join(values)}", f"35=D~{x}")
                rows.append(new_row)
        return rows
    return [row]

def expand_test_cases(row: Dict[str, str]) -> List[Dict[str, str]]:
    expanded = []
    update_parts = row["TagsToUpdate"].split(FIELD_DELIMITER)
    validate_parts = row["TagsToValidate"].split(FIELD_DELIMITER)

    multi_tag, multi_vals = None, []
    updates, validates = {}, {}

    for part in update_parts:
        if "=" not in part: continue
        tag, val = part.split("=", 1)
        if "~" in val and multi_tag is None and tag != "35":
            multi_tag = tag
            multi_vals = val.split(MULTI_VAL_DELIMITER)
        updates[tag] = val

    for part in validate_parts:
        if "=" not in part: continue
        tag, val = part.split("=", 1)
        validates[tag] = val

    updates = apply_tag_expansion(updates)
    validates = apply_tag_expansion(validates)

    for i in range(len(multi_vals) if multi_vals else 1):
        u = updates.copy()
        v = validates.copy()
        if multi_tag:
            u[multi_tag] = multi_vals[i]
            if multi_tag in validates:
                v[multi_tag] = validates[multi_tag].split(MULTI_VAL_DELIMITER)[i]
        expanded.append({
            "UseCaseID": row["UseCaseID"],
            "TestCaseID": row["TestCaseID"],
            "BaseMessage": row["BaseMessage"],
            "TagsToUpdate": u,
            "TagsToValidate": v
        })
    return expanded

# ---- Main Execution ----

def run_test(input_file: str):
    print(f"\nExecution ID: {execution_id}")
    print(f"Input: {input_file}")
    print(f"Result File: {result_out}")
    print(f"Summary File: {summary_out}")
    print(f"Log File: {log_file}")

    result_rows = []
    summary = {}
    sent_messages = {}

    with open(input_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for split_row in expand_35_dfg(row):
                test_cases = expand_test_cases(split_row)
                for case in test_cases:
                    ucid = case["UseCaseID"]
                    tcid = case["TestCaseID"]
                    updates = {k: resolve_placeholders(v, case["TagsToUpdate"], sent_messages) for k, v in case["TagsToUpdate"].items()}
                    tag11 = updates.get("11", f"{tcid}_{uuid.uuid4().hex[:4]}")
                    updates["11"] = tag11
                    fix = update_fix(case["BaseMessage"], updates)
                    msg_type = parse_fix(fix).get("35", "D")
                    sent_messages[tcid] = parse_fix(fix)

                    received = send_fix_message(fix, tag11, msg_type)
                    actual_tags = parse_fix(received, delimiter=SOD) if received else {}
                    validates = {k: resolve_placeholders(v, actual_tags, sent_messages) for k, v in case["TagsToValidate"].items()}
                    passed, reasons = validate_tags(validates, actual_tags, tcid, tag11)

                    print(f"[{ucid}/{tcid}] 11={tag11} - {'PASS' if passed else 'FAIL'}")

                    result_rows.append({
                        "UseCaseID": ucid,
                        "TestCaseID": tcid,
                        "ExecutionID": tag11,
                        "MessageType": msg_type,
                        "ValidationResult": "PASS" if passed else "FAIL",
                        "ValidationDetails": " | ".join(reasons),
                        "SentFixMessage": fix.replace(FIELD_DELIMITER, SOD),
                        "ReceivedFixMessage": received
                    })
                    summary.setdefault((ucid, tcid, msg_type), {"Total": 0, "Passed": 0, "Failed": 0})
                    summary[(ucid, tcid, msg_type)]["Total"] += 1
                    if passed:
                        summary[(ucid, tcid, msg_type)]["Passed"] += 1
                    else:
                        summary[(ucid, tcid, msg_type)]["Failed"] += 1

    with open(result_out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=result_rows[0].keys())
        writer.writeheader()
        writer.writerows(result_rows)

    with open(summary_out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["UseCaseID", "TestCaseID", "MessageType", "Total", "Passed", "Failed"])
        for (ucid, tcid, mt), stats in summary.items():
            writer.writerow([ucid, tcid, mt, stats["Total"], stats["Passed"], stats["Failed"]])

    print(f"\nResults saved to: {result_out}")
    print(f"Summary saved to: {summary_out}")
    print(f"Log saved to: {log_file}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_test_runner_base5_9_2.py <input_csv>")
        sys.exit(1)
    run_test(sys.argv[1])