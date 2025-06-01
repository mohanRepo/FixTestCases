#!/bin/bash
FIX_MSG="$1"
TEMP_FILE="logs/temp_msg.txt"
CURRENT_FILE="logs/Current"
echo "$FIX_MSG" > "$TEMP_FILE"
sleep 0.1
cp "$TEMP_FILE" "$CURRENT_FILE"
