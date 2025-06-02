import argparse
import csv
import logging
import os
import re
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path

# Configuration
FIELD_SEPARATOR = '|'
SOD = '\x01'  # Start of Day delimiter used in FIX messages
MAX_RETRIES = 3
WAIT_TIME = 0.3  # seconds between retries

# Directories
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / 'input'
OUTPUT_DIR = BASE_DIR / 'output'
LOG_DIR = OUTPUT_DIR

# Logging setup
EXECUTION_ID = datetime.utcnow().strftime("%Y%m%d%H%M%S")
LOG_FILE = LOG_DIR / f"test_run_{EXECUTION_ID}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='[%(levelname)s] %(message)s')

def parse_fix(fix_msg):
    return dict(tag_val.split('=') for tag_val in fix_msg.strip().split(SOD) if '=' in tag_val)

def build_fix(fix_dict):
    return SOD.join(f"{tag}={val}" for tag, val in fix_dict.items()) + SOD

def update_fix_tags(fix_msg, updates, test_case_id):
    fix_dict = parse_fix(fix_msg)

    # Apply updates and deletions
    for tag, val in updates.items():
        if val == '':
            fix_dict.pop(tag, None)
        else:
            fix_dict[tag] = val

    # Always set tags 11 and 52
    short_uuid = uuid.uuid4().hex[:4]
    fix_dict['11'] = f"{test_case_id}_{short_uuid}"
    fix_dict['52'] = datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')

    return build_fix(fix_dict), fix_dict['11'], fix_dict.get('35', '')

def validate_fix_message(fix_msg, validations):
    fix_dict = parse_fix(fix_msg)
    all_passed = True
    failed_tags = []

    for tag, expected_pattern in validations.items():
        actual_value = fix_dict.get(tag)
        if actual_value is None:
            logging.error(f"Validation failed: Tag {tag} missing.")
            failed_tags.append(f"{tag}=<missing>")
            all_passed = False
        elif not re.fullmatch(expected_pattern, actual_value):
            logging.error(f"Validation failed: Tag {tag} value '{actual_value}' does not match '{expected_pattern}'")
            failed_tags.append(f"{tag}={actual_value}")
            all_passed = False

    return all_passed, failed_tags

def run_fix_process(fix_msg, tag11, tag35):
    try:
        result = subprocess.run(
            ['./send_fix_message.sh', fix_msg],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode != 0:
            logging.error(f"Shell script failed: {result.stderr}")
            return None

        current_log_path = LOG_DIR / 'Current'
        for attempt in range(MAX_RETRIES):
            if current_log_path.exists():
                with open(current_log_path, 'r') as f:
                    for line in f:
                        if tag11 in line and f"35={tag35}" in line:
                            return line.strip()
            time.sleep(WAIT_TIME)

        logging.error(f"Processed message with tag11={tag11} and 35={tag35} not found after {MAX_RETRIES} retries.")
    except Exception as e:
        logging.error(f"Exception in running fix process: {e}")
    return None

def run_tests(input_file, result_file, summary_file):
    with open(input_file, 'r', newline='') as csvfile, \
         open(result_file, 'w', newline='') as result_out, \
         open(summary_file, 'w', newline='') as summary_out:

        reader = csv.DictReader(csvfile)
        result_writer = csv.writer(result_out)
        summary_writer = csv.writer(summary_out)

        result_writer.writerow(['UseCaseID', 'TestCaseID', 'Result', 'FIXMessage'])
        summary_writer.writerow(['UseCaseID', 'Passed', 'Failed', 'Total'])

        summary_stats = {}

        for row in reader:
            use_case_id = row['UseCaseID']
            test_case_id = row['TestCaseID']
            base_msg = row['BaseMessage'].replace(FIELD_SEPARATOR, SOD)

            updates = {}
            if row['TagsToUpdate']:
                updates = dict(tag_val.split('=') for tag_val in row['TagsToUpdate'].split(FIELD_SEPARATOR) if '=' in tag_val)

            validations = {}
            if row['TagsToValidate']:
                validations = dict(tag_val.split('=') for tag_val in row['TagsToValidate'].split(FIELD_SEPARATOR) if '=' in tag_val)

            updated_msg, tag11, tag35 = update_fix_tags(base_msg, updates, test_case_id)
            processed_msg = run_fix_process(updated_msg, tag11, tag35)

            if not processed_msg:
                result_writer.writerow([use_case_id, test_case_id, 'FAIL', updated_msg.replace(SOD, FIELD_SEPARATOR)])
                summary_stats.setdefault(use_case_id, {'Passed': 0, 'Failed': 0})['Failed'] += 1
                continue

            valid, failed_tags = validate_fix_message(processed_msg, validations)
            result_status = 'PASS' if valid else 'FAIL'
            if not valid:
                logging.error(f"Test {test_case_id} failed due to tags: {', '.join(failed_tags)}")

            result_writer.writerow([use_case_id, test_case_id, result_status, processed_msg.replace(SOD, FIELD_SEPARATOR)])
            key = summary_stats.setdefault(use_case_id, {'Passed': 0, 'Failed': 0})
            key['Passed' if valid else 'Failed'] += 1

        for uc_id, stats in summary_stats.items():
            total = stats['Passed'] + stats['Failed']
            summary_writer.writerow([uc_id, stats['Passed'], stats['Failed'], total])

        print("\n✅ Test execution complete.")
        print(f"📝 Summary written to: {summary_file}")

def main():
    global EXECUTION_ID
    EXECUTION_ID = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    parser = argparse.ArgumentParser(description="Run FIX message test cases.")
    parser.add_argument("input_file", help="Path to the input CSV file containing test cases")
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"❌ Input file not found: {input_path}")
        return

    summary_file = OUTPUT_DIR / f"{EXECUTION_ID}_summary.csv"
    result_file = OUTPUT_DIR / f"{EXECUTION_ID}_results.csv"

    run_tests(input_path, result_file, summary_file)

    print("\n📂 FILE PATHS")
    print(f"🔹 Input File     : {input_path}")
    print(f"🔹 Summary File   : {summary_file}")
    print(f"🔹 Result File    : {result_file}")
    print(f"🔹 Log File       : {LOG_FILE}")

if __name__ == "__main__":
    main()
