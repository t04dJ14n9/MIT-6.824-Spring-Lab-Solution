#!/bin/bash

# Check if a pattern is passed as an argument
if [ -z "$1" ]; then
    echo "Usage: $0 <test-pattern> <concurrency> <max_iterations>"
    exit 1
fi

pattern=$1
concurrency=$2
max_iterations=$3

# Default values if not provided
if [ -z "$concurrency" ]; then
    concurrency=1 # Default concurrency level
fi

if [ -z "$max_iterations" ]; then
    max_iterations=50 # Default number of iterations
fi

i=1
running_jobs=0

while [ $i -le $max_iterations ]; do
    # Run go test with the provided pattern and store output in a log file
    echo "Running test $i (concurrent job $((running_jobs + 1)))"
    go test -race -run "$pattern" >./logs/${pattern}_${i}.log &

    # Increment the number of running jobs
    running_jobs=$((running_jobs + 1))

    # Check if we have reached the concurrency limit
    if [ $running_jobs -ge $concurrency ]; then
        echo "Waiting for one of the running tests to finish..."
        wait -n # Wait for any one of the background jobs to finish
        running_jobs=$((running_jobs - 1))
    fi

    # Increment counter for the next iteration
    i=$((i + 1))
done

# Wait for all remaining background jobs to finish
wait
echo "All tests completed."
