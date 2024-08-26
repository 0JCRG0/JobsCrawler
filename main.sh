#!/bin/bash

echo "Starting script at $(date)" >> /root/JobsCrawler/logs/main_logger.log

# Load environment variables
set -a
source /root/JobsCrawler/.env
set +a

echo "Sourced .env" >> /root/JobsCrawler/logs/main_logger.log

source /root/JobsCrawler/venv/bin/activate && echo "Activated venv" >> /root/JobsCrawler/logs/main_logger.log

# Run the Python script with environment variables
/root/JobsCrawler/venv/bin/python /root/JobsCrawler/src/main.py 

echo "Finished script at $(date)" >> /root/JobsCrawler/logs/main_logger.log