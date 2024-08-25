#!/bin/bash

echo "Starting script at $(date)" >> /root/JobsCrawler/logs/debug.log
source /root/JobsCrawler/.env && echo "Sourced .env" >> /root/JobsCrawler/logs/debug.log
source /root/JobsCrawler/venv/bin/activate && echo "Activated venv" >> /root/JobsCrawler/logs/debug.log
/root/JobsCrawler/venv/bin/python /root/JobsCrawler/src/main.py 
echo "Finished script at $(date)" >> /root/JobsCrawler/logs/debug.log