#!/bin/bash

# Set PATH
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Change to the correct directory
cd /root/JobsCrawler || exit

echo "Starting script at $(date)" >> /root/JobsCrawler/logs/main_logger.log

# Load environment variables
set -a
source /root/JobsCrawler/.env
set +a

echo "Sourced .env" >> /root/JobsCrawler/logs/main_logger.log

# Activate virtual environment
source /root/JobsCrawler/venv/bin/activate

echo "Activated virtual environment" >> /root/JobsCrawler/logs/main_logger.log

# Run the Python script
python /root/JobsCrawler/src/main.py 

# Deactivate virtual environment
deactivate

echo "Finished script at $(date)" >> /root/JobsCrawler/logs/main_logger.log