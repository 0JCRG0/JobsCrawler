#!/bin/bash

# Set PATH
# PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Change to the correct directory
# cd /root/JobsCrawler || exit

# Create logs directory if it doesn't exist
mkdir -p logs

echo "Starting log script at $(date)" >> logs/logger.log

# Load environment variables
set -a
source .env
set +a

# Run the Python script and redirect its output to a log file
poetry run python src/logs_in_discord.py >> logs/logs_output.log 2>&1

echo "Finished script at $(date)" >> logs/logger.log