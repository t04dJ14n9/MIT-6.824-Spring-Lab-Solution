i=1
while true; do
    # Run go test and store output in a log file
    go test -race -run "2A|2B" > ./logs/2A2B_${i}.log
    
    # Check the previous log file for FAIL if it's not the first iteration
    if [ $i -gt 1 ]; then
        last_result=$(tail -n 1 ./logs/2A2B_$((i-1)).log)
        echo "Checking logs/2A2B_$((i-1)).log: $last_result"

        # If the last line contains "FAIL", stop the loop
        if echo "$last_result" | grep -q "FAIL"; then
            echo "Test failed in iteration $((i-1)), stopping..."
            break
        fi
    fi
    
    # Increment counter for the next iteration
    i=$((i+1))

    # Break the loop if i exceeds 1000
    if [ $i -gt 1000 ]; then
        echo "Reached 1000 iterations, stopping..."
        break
    fi
done

