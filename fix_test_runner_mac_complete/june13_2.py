import csv
import os
import re
import subprocess
import time
import uuid
import logging
from datetime import datetime
from typing import Dict, List

# ==== Configuration ====
SOH = "\x01"
FIELD_DELIMITER = "|"
MULTI_VAL_DELIMITER = "~"
LINUX_SCRIPT = "./send_fix_message.sh"
LOG_FILE_PATH = "logs/Current"
OUTPUT_DIR = "output"
WAIT_TIME_SEC = 0.3
RETRY_COUNT = 5

# ==== Setup Execution Context ====
os.makedirs(OUTPUT_DIR, exist_ok=True)
execution_id = datetime.now().strftime("%y%m%d_%H%M%S")
result_file = os.path.join(OUTPUT_DIR, f"test_result_{execution_id}.csv")
summary_file = os.path.join(OUTPUT_DIR, f"test_summary_{execution_id}.csv")
log_file = os.path.join(OUTPUT_DIR, f"fix_test_log_{execution_id}.log")

# ==== Logging ====
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==== Utility Functions ====
def parse_fix(fix_str: str) -> Dict[str, str]:
    return dict(item.split("=", 1) for item in fix_str.split(FIELD_DELIMITER) if "=" in item)

def build_fix(tags: Dict[str, str]) -> str:
    return FIELD_DELIMITER.join(f"{k}={v}" for k, v in tags.items())

def replace_placeholders(value: str, tag_map: Dict[str, str], global_sent_msgs: Dict[str, Dict[str, str]]) -> str:
    pattern = r"\$\{([^}]+)\}"
    def resolve(match):
        key = match.group(1)
        if "." in key:
            tc_id, tag = key.split(".")
            return global_sent_msgs.get(tc_id, {}).get(tag, "")
        return tag_map.get(key, "")
    return re.sub(pattern, resolve, value)

def update_fix(base_fix: str, updates: Dict[str, str], testcase_id: str, global_sent_msgs: Dict[str, Dict[str, str]]) -> str:
    tags = parse_fix(base_fix)
    for k, v in updates.items():
        resolved = replace_placeholders(v, tags, global_sent_msgs)
        if v == "":
            tags.pop(k, None)
        else:
            tags[k] = resolved
    if "52" not in tags:
        tags["52"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if "11" not in tags:
        tags["11"] = f"{testcase_id}_{uuid.uuid4().hex[:4]}"
    return build_fix(tags)

def send_fix_message(fix_msg: str) -> None:
    subprocess.run([LINUX_SCRIPT, fix_msg.replace(FIELD_DELIMITER, SOH)])

def grep_fix_message(tag11: str, msg_type: str) -> str:
    for _ in range(RETRY_COUNT):
        time.sleep(WAIT_TIME_SEC)
        with open(LOG_FILE_PATH, "r") as f:
            for line in f:
                if f"11={tag11}" in line and f"35={msg_type}" in line:
                    return line.strip()
    return ""

def expand_tags(entry: str) -> List[str]:
    if entry.startswith("[") and "]=" in entry:
        tags_str, val = entry[1:].split("]=")
        tags = tags_str.split("~")
        return [f"{tag}={val}" for tag in tags]
    return [entry]

def expand_test_cases(row: Dict[str, str]) -> List[Dict[str, str]]:
    updates = []
    for part in row["TagsToUpdate"].split(FIELD_DELIMITER):
        updates += expand_tags(part)
    update_dict = {}
    multi_tag, multi_values = None, []
    for part in updates:
        if "~" in part and "=" in part:
            k, v = part.split("=")
            multi_tag, multi_values = k, v.split(MULTI_VAL_DELIMITER)
        elif "=" in part:
            k, v = part.split("=")
            update_dict[k] = v
    validations = []
    for part in row["TagsToValidate"].split(FIELD_DELIMITER):
        validations += expand_tags(part)
    validate_dicts = []
    for i in range(len(multi_values) if multi_values else 1):
        vdict = {}
        for part in validations:
            if "=" not in part: continue
            k, v = part.split("=")
            if "~" in v:
                vals = v.split(MULTI_VAL_DELIMITER)
                vdict[k] = vals[i] if i < len(vals) else ""
            else:
                vdict[k] = v
        validate_dicts.append(vdict)
    cases = []
    for i, val in enumerate(multi_values or [""]):
        update = update_dict.copy()
        if multi_tag:
            update[multi_tag] = val
        cases.append({
            "UseCaseID": row["UseCaseID"],
            "TestCaseID": row["TestCaseID"],
            "BaseMessage": row["BaseMessage"],
            "TagsToUpdate": update,
            "TagsToValidate": validate_dicts[i],
            "ValidationResult": row.get("ValidationResult", "").strip().upper() != "FALSE"
        })
    return cases

def validate_tags(expected: Dict[str, str], actual: Dict[str, str]) -> (bool, List[str]):
    result, messages = True, []
    for tag, exp_val in expected.items():
        act_val = actual.get(tag)
        if exp_val == "":
            if act_val is not None:
                result = False
                messages.append(f"FAIL: Expected tag {tag} to be deleted, but found {act_val}")
            else:
                messages.append(f"PASS: Tag {tag} correctly deleted")
        elif re.fullmatch(exp_val, act_val or ""):
            messages.append(f"PASS: Tag {tag} matched {exp_val}")
        else:
            result = False
            messages.append(f"FAIL: Tag {tag} mismatch. Expected pattern {exp_val}, found {act_val}")
    return result, messages

# ==== Main Execution ====
def run_test(input_csv: str):
    test_results = []
    summary = {}
    sent_fix_map = {}

    with open(input_csv, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            test_cases = expand_test_cases(row)
            for case in test_cases:
                tcid = case["TestCaseID"]
                ucid = case["UseCaseID"]
                base_msg = case["BaseMessage"]
                updates = case["TagsToUpdate"]
                validates = case["TagsToValidate"]
                expected_fail = not case["ValidationResult"]

                updated_fix = update_fix(base_msg, updates, tcid, sent_fix_map)
                tag_map = parse_fix(updated_fix)
                tag11 = tag_map["11"]
                msg_type = tag_map.get("35", "NA")

                print(f"Running {ucid} / {tcid} / 11={tag11}")
                send_fix_message(updated_fix)
                received_fix = grep_fix_message(tag11, msg_type)
                actual_map = parse_fix(received_fix)

                passed, messages = validate_tags(validates, actual_map)
                true_result = passed != expected_fail
                print(f"Result: {'PASS' if true_result else 'FAIL'} - {' | '.join(messages)}")

                sent_fix_map[tcid] = tag_map
                test_results.append({
                    "UseCaseID": ucid,
                    "TestCaseID": tcid,
                    "MessageType": msg_type,
                    "ExecutionID": tag11,
                    "ValidationResult": "PASS" if true_result else "FAIL",
                    "SentFixMessage": updated_fix.replace(FIELD_DELIMITER, SOH),
                    "ReceivedFixMessage": received_fix
                })

                summary.setdefault((ucid, tcid, msg_type), {"Total": 0, "Passed": 0, "Failed": 0})
                summary[(ucid, tcid, msg_type)]["Total"] += 1
                if true_result:
                    summary[(ucid, tcid, msg_type)]["Passed"] += 1
                else:
                    summary[(ucid, tcid, msg_type)]["Failed"] += 1

    # Write result
    with open(result_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=test_results[0].keys())
        writer.writeheader()
        writer.writerows(test_results)

    # Write summary
    with open(summary_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["UseCaseID", "TestCaseID", "MessageType", "Total", "Passed", "Failed"])
        for key, val in summary.items():
            writer.writerow([*key, val["Total"], val["Passed"], val["Failed"]])

    print(f"\n✔️ Result File: {result_file}")
    print(f"✔️ Summary File: {summary_file}")
    print(f"✔️ Log File: {log_file}")

# Entry Point
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_test_runner_final.py <input_csv_file>")
    else:
        run_test(sys.argv[1])