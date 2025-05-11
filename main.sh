#!/bin/bash

# Set PATH
# PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Change to the correct directory
# cd /root/JobsCrawler || exit

# Create logs directory if it doesn't exist
mkdir -p logs

echo "Starting 'main.py' at $(date)" >> logs/main_logger.log

# Load environment variables
set -a
source .env
set +a

# Activate virtual environment
poetry install >> logs/main_logger.log 2>&1

# Run the Python script and redirect its output to a log file
poetry run python src/main.py >> logs/script_output.log 2>&1

echo "Finished script at $(date)" >> logs/main_logger.log