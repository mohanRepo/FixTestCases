import csv
import uuid
import logging
from datetime import datetime
from pathlib import Path

# -----------------------------
# Configuration
# -----------------------------
DELIMITER = '|'
MULTIVALUE_SEPARATOR = '~'
LOG_FILE = "fix_test_run.log"

# -----------------------------
# Logging Setup
# -----------------------------
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# -----------------------------
# Helper Functions
# -----------------------------
def parse_fix_message(fix_str, delimiter=DELIMITER):
    return dict(tag.split('=') for tag in fix_str.strip().split(delimiter) if '=' in tag)

def build_fix_message(tag_dict, delimiter=DELIMITER):
    return delimiter.join(f"{k}={v}" for k, v in tag_dict.items())

def expand_test_cases(tags_str):
    parts = tags_str.split(DELIMITER)
    multi_tag = None
    multi_values = []
    static_parts = {}
    for part in parts:
        if '=' in part:
            tag, val = part.split('=', 1)
            if MULTIVALUE_SEPARATOR in val:
                multi_tag = tag
                multi_values = val.split(MULTIVALUE_SEPARATOR)
            else:
                static_parts[tag] = val
    return multi_tag, multi_values, static_parts

def validate_tags(processed_dict, expected_str, testcase_id, tag11):
    parts = expected_str.split(DELIMITER)
    multi_tag = None
    multi_values = []
    static_parts = {}
    for part in parts:
        if '=' in part:
            tag, val = part.split('=', 1)
            if MULTIVALUE_SEPARATOR in val:
                multi_tag = tag
                multi_values = val.split(MULTIVALUE_SEPARATOR)
            else:
                static_parts[tag] = val

    validations = []
    for tag, val in static_parts.items():
        actual = processed_dict.get(tag)
        if actual != val:
            logging.warning(f"[{testcase_id}] Tag {tag} validation failed. Expected: {val}, Actual: {actual}")
            validations.append(False)
        else:
            logging.info(f"[{testcase_id}] Tag {tag} validated successfully with expected value {val}")
            validations.append(True)

    return validations, multi_tag, multi_values

# -----------------------------
# Main Function
# -----------------------------
def main(input_csv_path):
    base_name = Path(input_csv_path).stem
    result_file = f"{base_name}_result.csv"
    summary_file = f"{base_name}_summary.csv"

    with open(input_csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        result_rows = []
        summary_rows = []

        for row in reader:
            use_case_id = row["UseCaseID"]
            testcase_base = row["TestCaseID"]
            base_msg = row["BaseMessage"]
            tags_to_update = row["TagsToUpdate"]
            tags_to_validate = row["TagsToValidate"]

            update_multi_tag, update_values, update_static = expand_test_cases(tags_to_update)
            validate_multi_tag, validate_values, validate_static = expand_test_cases(tags_to_validate)

            for i, update_val in enumerate(update_values):
                test_id = f"{testcase_base}_{chr(ord('A') + i)}"
                tag11 = f"{test_id}_{uuid.uuid4().hex[:4]}"

                msg_dict = parse_fix_message(base_msg)
                msg_dict.update(update_static)
                msg_dict[update_multi_tag] = update_val
                msg_dict["11"] = tag11
                msg_dict["52"] = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]

                sent_msg = build_fix_message(msg_dict)

                # Simulated processing (actual implementation would call a Linux process and read logs)
                processed_dict = msg_dict.copy()
                processed_dict[update_multi_tag] = validate_values[i]
                processed_dict.update(validate_static)
                processed_msg = build_fix_message(processed_dict)

                # Validation
                validations, _, _ = validate_tags(processed_dict, tags_to_validate, test_id, tag11)
                status = "PASS" if all(validations) else "FAIL"

                logging.info(f"[{test_id}] Final result: {status}")

                result_rows.append({
                    "UseCaseID": use_case_id,
                    "TestCaseID": test_id,
                    "Tag11": tag11,
                    "SentFIX": sent_msg,
                    "ProcessedFIX": processed_msg,
                    "ValidationResult": status
                })

                summary_rows.append({
                    "UseCaseID": use_case_id,
                    "TestCaseID": test_id,
                    "Tag11": tag11,
                    "ValidationResult": status
                })

    # Write result CSV
    with open(result_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=result_rows[0].keys())
        writer.writeheader()
        writer.writerows(result_rows)

    # Write summary CSV
    with open(summary_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"\n✅ Process completed.\nInput File: {input_csv_path}\nResult File: {result_file}\nSummary File: {summary_file}\nLog File: {LOG_FILE}")

# -----------------------------
# Script Entry Point
# -----------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_test_runner.py <input_csv_file>")
    else:
        main(sys.argv[1])
