#!/bin/bash

# Iterate over all files in the current directory

for file in *; do  if [ -f "$file" ]; then echo $file $(tail -n 1 "$file"); fi; done | grep FAIL