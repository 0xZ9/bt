#!/bin/bash

# Function to kill child processes
function kill_processes {
    for job in $(jobs -p); do
        kill -SIGKILL $job
    done
}

# Handle signals
trap 'echo "Aborting..."; kill_processes; exit 1' SIGINT SIGTERM

for file in conf_*; do
    echo "Processing $file"
    until python3 -m demeter.downloader "$file"; do
      echo "Download failed for $file, retrying in 10 seconds..."
      sleep 10
    done &
done

# Wait for all parallel jobs to finish
wait
