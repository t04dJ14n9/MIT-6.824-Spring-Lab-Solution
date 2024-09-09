#!/bin/bash

ok_count=0
# Iterate over all files in the current directory
for file in *; do  
  if [ -f "$file" ]; then
    # Check if the last 3 lines contain the word "FAIL"
    if tail -n 3 "$file" | grep -q "FAIL"; then
      echo "File: $file contains FAIL in the last 3 lines."
    fi

    if tail -n 3 "$file" | grep -q "ok"; then
      ok_count=$((ok_count + 1))
    fi
  fi
done

echo "Number of test passed: $ok_count"