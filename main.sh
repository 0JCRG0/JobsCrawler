#!/bin/bash

# Set PATH
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Change to the correct directory
cd /root/JobsCrawler || exit

echo "Starting 'main.py' at $(date)" >> /root/JobsCrawler/logs/main_logger.log

# Load environment variables
set -a
source /root/JobsCrawler/.env
set +a

# Activate virtual environment
source /root/JobsCrawler/venv/bin/activate

# Run the Python script and redirect its output to a log file
python /root/JobsCrawler/src/main.py >> /root/JobsCrawler/logs/script_output.log 2>&1

# Deactivate virtual environment
deactivate

echo "Finished script at $(date)" >> /root/JobsCrawler/logs/main_logger.log