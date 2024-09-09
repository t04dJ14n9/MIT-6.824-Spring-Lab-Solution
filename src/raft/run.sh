#!/bin/bash

# Check if a pattern is passed as an argument
if [ -z "$1" ]; then
    echo "Usage: $0 <test-pattern> <max_iteration>"
    exit 1
fi

pattern=$1
i=1

if [ -n "$2" ]; then
    max_iterations=$2
else
    max_iterations=50
fi

while true; do
   
    # Check the previous log file for FAIL if it's not the first iteration
    if [ $i -gt 1 ]; then
        last_result=$(tail -n 1 ./logs/${pattern}_${i-1}.log)
        echo "Checking logs/${pattern}_${i-1}.log: $last_result"

        # If the last line contains "FAIL", stop the loop
        if echo "$last_result" | grep -q "FAIL"; then
            echo "Test failed in iteration ${i-1}, stopping..."
            break
        fi
    fi

     # Run go test with the provided pattern and store output in a log file
    go test -race -run "$pattern" > ./logs/${pattern}_${i}.log


    # Stop the loop if max_iterations is reached
    if [ $i -ge $max_iterations ]; then
        echo "Reached the maximum iteration limit of $max_iterations, stopping..."
        break
    fi

    # Increment counter for the next iteration
    i=$((i+1))
done 