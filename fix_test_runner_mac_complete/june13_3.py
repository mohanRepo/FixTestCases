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
SOH = "\x01"
FIELD_DELIMITER = "|"
MULTI_VAL_DELIMITER = "~"
TAG_GROUP_PATTERN = re.compile(r"\[(.*?)\]=([^|]*)")
PLACEHOLDER_PATTERN = re.compile(r"\${(.*?)}")

RETRY_COUNT = 5
RETRY_WAIT = 0.3  # seconds

OUTPUT_DIR = "output"
LOG_DIR = "logs"
LINUX_PROCESS_SCRIPT = "./send_fix_message.sh"
CURRENT_LOG_FILE = os.path.join(LOG_DIR, "Current")

execution_id = datetime.now().strftime("%y%m%d_%H%M%S")
result_file = os.path.join(OUTPUT_DIR, f"test_result_{execution_id}.csv")
summary_file = os.path.join(OUTPUT_DIR, f"test_summary_{execution_id}.csv")
log_file = os.path.join(OUTPUT_DIR, f"fix_test_run_{execution_id}.log")

os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

sent_fix_by_testcase = {}

def parse_fix(fix_str: str) -> Dict[str, str]:
    return dict(item.split("=", 1) for item in fix_str.split(FIELD_DELIMITER) if "=" in item)

def build_fix(tags: Dict[str, str], use_soh=False) -> str:
    delimiter = SOH if use_soh else FIELD_DELIMITER
    return delimiter.join(f"{k}={v}" for k, v in tags.items())

def expand_tag_groups(input_str: str) -> str:
    def replacer(match):
        tags = match.group(1).split("~")
        value = match.group(2)
        return "|".join(f"{tag}={value}" for tag in tags)
    return TAG_GROUP_PATTERN.sub(replacer, input_str)

def expand_multivalues(tag_value_str: str) -> List[Dict[str, str]]:
    tag_value_str = expand_tag_groups(tag_value_str)
    tags = tag_value_str.strip().split("|")
    multi_tag = None
    multi_values = []
    static_tags = {}

    for t in tags:
        if "~" in t:
            tag, vals = t.split("=")
            multi_tag = tag
            multi_values = vals.split(MULTI_VAL_DELIMITER)
        else:
            if "=" in t:
                tag, val = t.split("=")
                static_tags[tag] = val

    if not multi_tag:
        return [static_tags]

    expanded = []
    for val in multi_values:
        case = static_tags.copy()
        case[multi_tag] = val
        expanded.append(case)
    return expanded

def resolve_placeholders(value: str, current_tags: Dict[str, str]) -> str:
    def replacer(match):
        placeholder = match.group(1)
        if "." in placeholder:
            ref_test, ref_tag = placeholder.split(".")
            return parse_fix(sent_fix_by_testcase.get(ref_test, "")).get(ref_tag, "")
        else:
            return current_tags.get(placeholder, "")
    return PLACEHOLDER_PATTERN.sub(replacer, value)

def update_fix(base_fix: str, updates: Dict[str, str], test_case_id: str) -> str:
    tags = parse_fix(base_fix)
    tags.update({k: v for k, v in updates.items() if v != ""})
    for k, v in updates.items():
        if v == "":
            tags.pop(k, None)
    if "11" not in updates:
        tags["11"] = f"{test_case_id}_{uuid.uuid4().hex[:4]}"
    tags["52"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return build_fix(tags)

def send_fix_message(fix_msg: str, tag11: str, msg_type: str) -> str:
    subprocess.run([LINUX_PROCESS_SCRIPT, fix_msg])
    for _ in range(RETRY_COUNT):
        time.sleep(RETRY_WAIT)
        with open(CURRENT_LOG_FILE, "r") as f:
            for line in f:
                if f"11={tag11}" in line and f"35={msg_type}" in line:
                    return line.strip()
    return ""

def validate_tags(expected: Dict[str, str], actual: Dict[str, str], test_case_id: str, tag11: str) -> (bool, List[str]):
    result = True
    messages = []
    for tag, exp_val in expected.items():
        act_val = actual.get(tag)
        if exp_val == "":
            if act_val is not None:
                result = False
                messages.append(f"FAIL: Tag {tag} should be deleted but found {act_val} [TC: {test_case_id}, 11={tag11}]")
            else:
                messages.append(f"PASS: Tag {tag} correctly deleted [TC: {test_case_id}, 11={tag11}]")
        else:
            if not re.fullmatch(exp_val, act_val or ""):
                result = False
                messages.append(f"FAIL: Tag {tag} mismatch. Expected pattern: {exp_val}, Actual: {act_val} [TC: {test_case_id}, 11={tag11}]")
            else:
                messages.append(f"PASS: Tag {tag} matched value {act_val} [TC: {test_case_id}, 11={tag11}]")
    return result, messages

def expand_35_logic(tags_to_update: str) -> List[str]:
    parts = tags_to_update.split("|")
    for p in parts:
        if p.startswith("35="):
            values = p[3:].split(MULTI_VAL_DELIMITER)
            if set(values) >= {"D", "F", "G"}:
                d_f = tags_to_update.replace("35=D~F~G", "35=D~F")
                d_g = tags_to_update.replace("35=D~F~G", "35=D~G")
                return [d_f, d_g]
    return [tags_to_update]

def expand_test_cases(row: Dict[str, str]) -> List[Dict[str, any]]:
    update_variants = []
    for update in expand_35_logic(row["TagsToUpdate"]):
        update_variants.extend(expand_multivalues(update))

    validate_variants = expand_multivalues(row["TagsToValidate"])

    expanded = []
    for i in range(len(update_variants)):
        case = {
            "UseCaseID": row["UseCaseID"],
            "TestCaseID": row["TestCaseID"],
            "BaseMessage": row["BaseMessage"],
            "TagsToUpdate": update_variants[i],
            "TagsToValidate": validate_variants[i] if i < len(validate_variants) else {},
            "ValidationResult": row.get("ValidationResult", "true").strip().lower()
        }
        expanded.append(case)

        # Handle child for 35=F or 35=G
        msg_type = update_variants[i].get("35", "")
        if msg_type in {"F", "G"}:
            parent_tag11 = update_variants[i].get("41") or ""
            case["TagsToUpdate"]["41"] = parent_tag11
            case["TagsToUpdate"]["11"] = f"{case['TestCaseID']}_{uuid.uuid4().hex[:4]}"
    return expanded

def run_test(input_file: str):
    print(f"\n📄 Input: {input_file}")
    print(f"📤 Result: {result_file}")
    print(f"📊 Summary: {summary_file}")
    print(f"🪵 Log: {log_file}\n")

    total, passed, failed = 0, 0, 0
    result_rows = []
    summary_data = {}

    with open(input_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            expanded_cases = expand_test_cases(row)
            for case in expanded_cases:
                total += 1
                test_id = case["TestCaseID"]
                usecase_id = case["UseCaseID"]
                updates = {k: resolve_placeholders(v, case["TagsToUpdate"]) for k, v in case["TagsToUpdate"].items()}
                fix_msg = update_fix(case["BaseMessage"], updates, test_id)
                tag11 = parse_fix(fix_msg).get("11", "")
                msg_type = parse_fix(fix_msg).get("35", "")
                sent_fix_by_testcase[test_id] = fix_msg

                print(f"🧪 {usecase_id}-{test_id} | 11={tag11} | 35={msg_type}")

                sent = build_fix(parse_fix(fix_msg), use_soh=True)
                received = send_fix_message(sent, tag11, msg_type)
                received_tags = parse_fix(received) if received else {}

                is_pass, messages = validate_tags(case["TagsToValidate"], received_tags, test_id, tag11)

                # Handle expectation flip
                expected_result = case.get("ValidationResult", "true") == "true"
                final_result = is_pass == expected_result
                if final_result:
                    passed += 1
                    print(f"✅ PASS {test_id}")
                    logging.info(f"{test_id} - PASS")
                else:
                    failed += 1
                    print(f"❌ FAIL {test_id}")
                    logging.error(f"{test_id} - FAIL: {messages}")

                result_rows.append({
                    "UseCaseID": usecase_id,
                    "TestCaseID": test_id,
                    "Tag11": tag11,
                    "MessageType": msg_type,
                    "ValidationResult": "PASS" if final_result else "FAIL",
                    "ExpectedResult": "PASS" if expected_result else "FAIL",
                    "ValidationDetails": " | ".join(messages),
                    "SentFixMessage": sent,
                    "ReceivedFixMessage": received or ""
                })

                # Summary by UseCase + MsgType
                key = (usecase_id, msg_type)
                if key not in summary_data:
                    summary_data[key] = {"Total": 0, "Passed": 0, "Failed": 0}
                summary_data[key]["Total"] += 1
                if final_result:
                    summary_data[key]["Passed"] += 1
                else:
                    summary_data[key]["Failed"] += 1

    # Write test result
    with open(result_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=result_rows[0].keys())
        writer.writeheader()
        writer.writerows(result_rows)

    # Write summary
    with open(summary_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["UseCaseID", "MessageType", "Total", "Passed", "Failed"])
        for (ucid, msgtype), data in summary_data.items():
            writer.writerow([ucid, msgtype, data["Total"], data["Passed"], data["Failed"]])

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_test_runner_base6.py <input_csv_file>")
    else:
        run_test(sys.argv[1])