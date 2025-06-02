#!/usr/bin/env python3
import csv
import re
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
import logging
import sys

# =================== CONFIGURATION ===================
DELIMITER = '|'  # Custom FIX field separator used in base/test files
SOH = '\x01'     # Standard FIX protocol separator for real transmission

# Setup directories
BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Generate execution ID
EXECUTION_ID = uuid.uuid4().hex[:8]
RESULT_FILE = OUTPUT_DIR / f"results_{EXECUTION_ID}.csv"
SUMMARY_FILE = OUTPUT_DIR / f"summary_{EXECUTION_ID}.csv"
CURRENT_LOG = LOGS_DIR / "Current"
MOCK_SENDER = BASE_DIR / "send_fix_message.sh"

# Setup logging
logging.basicConfig(
    filename=BASE_DIR / f"test_run_{EXECUTION_ID}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =================== HELPER FUNCTIONS ===================

def parse_fix(fix_str, separator=DELIMITER):
    return dict(field.split('=', 1) for field in fix_str.strip().split(separator) if '=' in field)

def build_fix(fix_dict, separator=DELIMITER):
    return separator.join(f"{k}={v}" for k, v in fix_dict.items())

def update_fix_tags(fix_msg, updates):
    fix_dict = parse_fix(fix_msg)

    # Apply updates and deletions
    for tag, val in updates.items():
        if val == '':
            fix_dict.pop(tag, None)  # Delete the tag
        else:
            fix_dict[tag] = val

    # Always update tags 11 and 52
    fix_dict['11'] = f"TestRun_{EXECUTION_ID}_{uuid.uuid4().hex[:4]}"
    fix_dict['52'] = datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')

    return build_fix(fix_dict)

def validate_tags(fix_msg, validations, logger, test_case_id):
    fix_dict = parse_fix(fix_msg)
    all_passed = True

    for tag, expected_pattern in validations.items():
        actual_value = fix_dict.get(tag)
        if actual_value is None:
            logger.error(f"{test_case_id} - Validation failed for tag {tag}: expected pattern '{expected_pattern}', tag not found")
            all_passed = False
        elif not re.fullmatch(expected_pattern, actual_value):
            logger.error(f"{test_case_id} - Validation failed for tag {tag}: expected pattern '{expected_pattern}', actual value '{actual_value}'")
            all_passed = False

    return all_passed

def grep_log(tag11, tag35, retries=3, delay=0.5):
    for _ in range(retries):
        if CURRENT_LOG.exists():
            with CURRENT_LOG.open() as f:
                for line in f:
                    if f"11={tag11}" in line and f"35={tag35}" in line:
                        return line.strip()
        time.sleep(delay)
    return ""

# =================== CORE TEST RUN ===================

def run_test_case(row):
    use_case_id = row["UseCaseID"]
    test_case_id = row["TestCaseID"]
    base_fix = row["BaseFIXMessage"]
    updates = dict(tag.split('=', 1) for tag in row["TagsToUpdate"].split(DELIMITER) if '=' in tag)
    validations = dict(tag.split('=', 1) for tag in row["TagsToValidate"].split(DELIMITER) if '=' in tag)
    expected = row["ExpectedValidationResult"]

    updated_fix = update_fix_tags(base_fix, updates)
    logging.info(f"{test_case_id}: Updated FIX: {updated_fix}")

    soh_fix = updated_fix.replace(DELIMITER, SOH)

    try:
        subprocess.run([str(MOCK_SENDER), soh_fix], check=True)
    except Exception as e:
        logging.error(f"{test_case_id}: Error sending FIX - {e}")
        return [use_case_id, test_case_id, updated_fix, "", "FAIL", expected]

    tag11 = parse_fix(updated_fix).get("11")
    tag35 = parse_fix(updated_fix).get("35")
    processed_fix = grep_log(tag11, tag35)

    if not processed_fix:
        logging.warning(f"{test_case_id}: No match found in logs")
        return [use_case_id, test_case_id, updated_fix, "", "FAIL", expected]

    result = "PASS" if validate_tags(processed_fix, validations, logging , test_case_id) else "FAIL"
    processed_pipe = processed_fix.replace(SOH, DELIMITER)
    return [use_case_id, test_case_id, updated_fix, processed_pipe, result, expected]

def main(input_csv):
    with open(input_csv, newline='') as f:
        reader = csv.DictReader(f)
        results = [run_test_case(row) for row in reader]

    headers = ["UseCaseID", "TestCaseID", "UpdatedFIX", "ProcessedFIX", "ValidationResult", "ExpectedValidationResult"]
    with open(RESULT_FILE, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(results)

    summary = {}
    for row in results:
        ucid = row[0]
        outcome = row[4]
        summary.setdefault(ucid, {"PASS": 0, "FAIL": 0})
        summary[ucid][outcome] += 1

    with open(SUMMARY_FILE, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["UseCaseID", "PASS", "FAIL", "TOTAL"])
        for ucid, counts in summary.items():
            total = counts["PASS"] + counts["FAIL"]
            writer.writerow([ucid, counts["PASS"], counts["FAIL"], total])

    print(f"Results: {RESULT_FILE}")
    print(f"Summary: {SUMMARY_FILE}")
    logging.info("Test run complete")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 fix_test_runner.py <test_cases.csv>")
    else:
        main(sys.argv[1])
