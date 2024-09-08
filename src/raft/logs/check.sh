#!/bin/bash

# Iterate over all files in the current directory

for file in *; do  
  if [ -f "$file" ]; then 
    grep "FAIL: Test" "$file" && echo -n "File: $file"
    grep "RACE" "$file" && echo -n "File: $file"
    grep "Race" "$file" && echo -n "File: $file"
    grep "race" "$file" && echo -n "File: $file"
  fi
done