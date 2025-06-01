import csv
import uuid
import time
import subprocess
import re
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Directories
LOG_DIR = Path("logs")
OUTPUT_DIR = Path("output")
LOG_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Files
CURRENT_FILE = LOG_DIR / "Current"
LOG_FILE = OUTPUT_DIR / "fix_test_runner.log"

# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DELIMITER = '\x01'

def parse_fix_message(msg):
    return dict(tag.split("=", 1) for tag in msg.strip().split(DELIMITER) if "=" in tag)

def build_fix_message(msg, updates, testcase_id):
    fix_dict = parse_fix_message(msg.replace("|", DELIMITER))
    fix_dict['11'] = f"Run_{testcase_id}_{uuid.uuid4().hex[:8]}"
    fix_dict['52'] = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
    for pair in updates.split(";"):
        if "=" in pair:
            k, v = pair.split("=", 1)
            fix_dict[k] = v
    return DELIMITER.join(f"{k}={v}" for k, v in fix_dict.items())

def validate_fix_message(msg, validation_rules):
    fix_dict = parse_fix_message(msg)
    for rule in validation_rules.split(";"):
        if "=" in rule:
            k, expected = rule.split("=", 1)
            actual = fix_dict.get(k)
            if expected == "":
                if actual is not None:
                    return False
            elif actual is None or not re.fullmatch(expected, actual):
                return False
    return True

def run_mock_script(fix_msg):
    subprocess.run(["bash", "./send_fix_message.sh", fix_msg], check=True)

def extract_from_log(tag11, tag35, retries=3, delay=0.2):
    for _ in range(retries):
        if CURRENT_FILE.exists():
            with open(CURRENT_FILE, "r") as f:
                for line in f:
                    if tag11 in line and f"35={tag35}" in line:
                        return line.strip()
        time.sleep(delay)
    return ""

def main(input_csv):
    execution_id = f"TestRun_{uuid.uuid4().hex[:6]}"
    results = []
    summary = defaultdict(lambda: {"Total": 0, "Passed": 0, "Failed": 0})

    logging.info(f"Starting test run: ExecutionID={execution_id}")

    with open(input_csv, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            use_case_id = row["UseCaseID"]
            test_case_id = row["TestCaseID"]
            base_msg = row["BaseFIXMessage"]
            updates = row["TagsToUpdate"]
            validations = row["TagsToValidate"]
            expected = row["ExpectedValidationResult"]

            logging.info(f"Processing TestCaseID={test_case_id} (UseCaseID={use_case_id})")

            updated_msg = build_fix_message(base_msg, updates, test_case_id)
            tag11 = re.search(r"11=([^\x01]+)", updated_msg).group(1)
            tag35 = re.search(r"35=([^\x01]+)", updated_msg).group(1)

            run_mock_script(updated_msg)
            logging.info(f"Sent FIX message with tag 11={tag11}")

            actual_msg = extract_from_log(tag11, tag35)
            logging.info(f"Retrieved message from log for tag 11={tag11}")

            actual_result = "PASS" if validate_fix_message(actual_msg, validations) else "FAIL"
            logging.info(f"Validation {actual_result} for TestCaseID={test_case_id}")

            results.append({
                "ExecutionID": execution_id,
                "UseCaseID": use_case_id,
                "TestCaseID": test_case_id,
                "UpdatedMessage": updated_msg,
                "ActualMessage": actual_msg,
                "Result": actual_result
            })

            summary[use_case_id]["Total"] += 1
            summary[use_case_id]["Passed"] += int(actual_result == "PASS")
            summary[use_case_id]["Failed"] += int(actual_result == "FAIL")

    # Write result file
    result_file = OUTPUT_DIR / f"{execution_id}_results.csv"
    with open(result_file, "w", newline='') as f:
        fieldnames = ["ExecutionID", "UseCaseID", "TestCaseID", "UpdatedMessage", "ActualMessage", "Result"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # Write summary file
    summary_file = OUTPUT_DIR / f"{execution_id}_summary.csv"
    with open(summary_file, "w", newline='') as f:
        fieldnames = ["ExecutionID", "UseCaseID", "Total", "Passed", "Failed"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ucid, stats in summary.items():
            writer.writerow({
                "ExecutionID": execution_id,
                "UseCaseID": ucid,
                **stats
            })

    logging.info(f"✅ Test run completed. Results: {result_file}, Summary: {summary_file}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python fix_test_runner.py test_cases.csv")
    else:
        main(sys.argv[1])
