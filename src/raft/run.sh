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
    # Run go test with the provided pattern and store output in a log file
    go test -race -run "$pattern" >./logs/${pattern}_${i}.log

    # Stop the loop if max_iterations is reached
    if [ $i -ge $max_iterations ]; then
        echo "Reached the maximum iteration limit of $max_iterations, stopping..."
        break
    fi

    # Increment counter for the next iteration
    i=$((i + 1))
done
