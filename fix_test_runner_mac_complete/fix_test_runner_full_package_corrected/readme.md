
# FIX Test Runner

## Overview
This script automates the testing of FIX protocol messages by reading an input CSV file, updating FIX tags, sending the messages to a Linux process, and validating the responses.

## How It Works
- **Input CSV**: Define test cases with base FIX messages, tag updates, and validation checks.
- **Tag Updates**:
  - Use `|` as a field delimiter.
  - Use `~` to define multiple values for a tag (expanding multiple test cases).
- **Special 35 Handling**:
  - If `35=D~G` or `35=D~F` is specified, a `G` or `F` message is auto-generated for each `D` with `41` set to `D`'s `11`.
- **Tag 11**:
  - If specified in the input, it will be used.
  - Otherwise, a unique `11` is auto-generated.
- **Tag 52**:
  - Always generated as UTC in `YYYYMMDD-HH:MM:SS` format.
- **Validation**:
  - Validates tag values using regex matching.
  - Empty value in `TagsToUpdate` deletes the tag.
  - Empty value in `TagsToValidate` expects the tag to be missing.

## Input CSV Format
| Column | Description |
|-------|-------------|
| `UseCaseID` | Use case identifier |
| `TestCaseID` | Test case identifier |
| `BaseMessage` | Base FIX message with `|` delimiter |
| `TagsToUpdate` | Tags to update, `~` to specify multiple values |
| `TagsToValidate` | Tags to validate, `~` to specify multiple expected values |

## Running the Script
```bash
python fix_test_runner_final.py sample_input_final.csv
```

## Output Files
Generated under the `output/` directory:
- `test_result_<timestamp>.csv`: Detailed test results.
- `test_summary_<timestamp>.csv`: Summary per use case.
- `fix_test_run_<timestamp>.log`: Execution logs.

## Example
Input CSV sample:
```
UseCaseID,TestCaseID,BaseMessage,TagsToUpdate,TagsToValidate
UC01,TC101,8=FIX.4.2|35=D|49=TRDR|56=BRKR,35=D~G|1001=A~B~C,1001=A~B~C|59=2
```

## Notes
- Messages are sent with `\x01` (SOD) delimiter to the Linux process.
- Script expects the Linux process to write responses to `./logs/Current` file.
- Always ensure proper permissions for `send_fix_message.sh` and `logs/` directory.
