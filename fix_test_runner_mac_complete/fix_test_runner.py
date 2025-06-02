import csv
import os
import re
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
import logging

# ========= CONFIGURATION ========= #
DELIMITER = '|'
SOH = '\x01'
EXECUTION_ID = datetime.utcnow().strftime('%Y%m%d%H%M%S')
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = OUTPUT_DIR / f"test_run_{EXECUTION_ID}.log"
RESULT_FILE = OUTPUT_DIR / f"results_{EXECUTION_ID}.csv"
SUMMARY_FILE = OUTPUT_DIR / f"summary_{EXECUTION_ID}.csv"
MOCK_SH_PATH = BASE_DIR / "send_fix_message.sh"
CURRENT_LOG_FILE = LOG_DIR / "Current"

RETRY_COUNT = 3
RETRY_WAIT = 0.5  # seconds
# ================================= #

# Setup directories
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Setup logger
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger()


def parse_fix(msg, delimiter=DELIMITER):
    return dict(field.split('=', 1) for field in msg.strip().split(delimiter) if '=' in field)


def build_fix(fix_dict, delimiter=DELIMITER):
    return delimiter.join(f"{k}={v}" for k, v in fix_dict.items()) + delimiter


def update_fix_tags(fix_msg, updates):
    fix_dict = parse_fix(fix_msg)

    # Apply updates and deletions
    for tag, val in updates.items():
        if val == '':
            fix_dict.pop(tag, None)  # Delete tag
        else:
            fix_dict[tag] = val

    # Always set tags 11 and 52
    fix_dict['11'] = f"TestRun_{EXECUTION_ID}_{uuid.uuid4().hex[:4]}"
    fix_dict['52'] = datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')

    return build_fix(fix_dict), fix_dict['11'], fix_dict['35']


def send_to_mock(fix_message):
    soh_message = fix_message.replace(DELIMITER, SOH)
    subprocess.run([str(MOCK_SH_PATH), soh_message], check=True)


def retrieve_processed_msg(tag11, tag35):
    for _ in range(RETRY_COUNT):
        time.sleep(RETRY_WAIT)
        if CURRENT_LOG_FILE.exists():
            with open(CURRENT_LOG_FILE) as f:
                lines = f.read().split(SOH)
                # Reconstruct messages from flat list
                grouped = []
                temp = []
                for field in lines:
                    if field.startswith("8=") and temp:
                        grouped.append(temp)
                        temp = []
                    temp.append(field)
                if temp:
                    grouped.append(temp)

                for msg_fields in grouped:
                    fix = {f.split('=')[0]: f.split('=')[1] for f in msg_fields if '=' in f}
                    if fix.get('11') == tag11 and fix.get('35') == tag35:
                        return DELIMITER.join(f"{k}={v}" for k, v in fix.items()) + DELIMITER
    return None


def validate_fix_message(fix_msg, validations, logger, test_case_id):
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


def run_tests(input_file_path):
    results = []
    summary = {}

    with open(input_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            use_case = row['UseCaseID']
            test_case_id = row['TestCaseID']
            base_msg = row['BaseFIXMessage']
            updates = dict(field.split('=', 1) for field in row['TagsToUpdate'].split(DELIMITER) if '=' in field)
            validations = dict(field.split('=', 1) for field in row['TagsToValidate'].split(DELIMITER) if '=' in field)
            expected = row['ExpectedValidationResult'].strip().upper()

            logger.info(f"Running test case {test_case_id} in use case {use_case}")

            updated_msg, tag11, tag35 = update_fix_tags(base_msg, updates)
            logger.info(f"Updated FIX Message: {updated_msg}")

            try:
                send_to_mock(updated_msg)
            except Exception as e:
                logger.error(f"{test_case_id} - Failed to send message: {e}")
                results.append([use_case, test_case_id, updated_msg, "ERROR", "SEND_FAIL"])
                continue

            processed_msg = retrieve_processed_msg(tag11, tag35)
            if not processed_msg:
                logger.error(f"{test_case_id} - Processed message not found in logs for tag11={tag11}")
                results.append([use_case, test_case_id, updated_msg, "ERROR", "MSG_NOT_FOUND"])
                continue

            logger.info(f"Processed FIX Message: {processed_msg}")

            is_valid = validate_fix_message(processed_msg, validations, logger, test_case_id)
            actual_result = "PASS" if is_valid else "FAIL"
            status = "MATCH" if actual_result == expected else "MISMATCH"

            results.append([use_case, test_case_id, processed_msg, actual_result, status])

            if use_case not in summary:
                summary[use_case] = {"PASS": 0, "FAIL": 0}
            summary[use_case][actual_result] += 1

    # Write results
    with open(RESULT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['UseCaseID', 'TestCaseID', 'ProcessedFIXMessage', 'ActualResult', 'Comparison'])
        writer.writerows(results)

    # Write summary
    with open(SUMMARY_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['UseCaseID', 'Passed', 'Failed'])
        for uc, stats in summary.items():
            writer.writerow([uc, stats["PASS"], stats["FAIL"]])

    logger.info(f"Test run completed. Results saved to {RESULT_FILE}, summary to {SUMMARY_FILE}")
