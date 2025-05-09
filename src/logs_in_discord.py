import json
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
LOG_FILE = "logs/script_output.log"
BATCH_SIZE = 10  
RATE_LIMIT_DELAY_UPPER_BOUND = 5
PATTERNS = {
        "ERROR": r"ERROR",
        "WARNING": r"WARN"
    }
MAX_LOGS_TO_SEND = 30
DEFAULT_MINUTES = 45
# 0.25 GB in bytes
FILE_SIZE_WARNING_THRESHOLD = 0.25 * 1024 * 1024 * 1024
# 0.5 GB in bytes  
FILE_SIZE_DELETE_THRESHOLD = 0.5 * 1024 * 1024 * 1024  

def _random_sleep():   
    time_to_sleep = random.randint(1, RATE_LIMIT_DELAY_UPPER_BOUND)
    return time.sleep(time_to_sleep)

def check_file_size():
    """Check log file size and take action if it exceeds thresholds."""
    file_size = os.path.getsize(LOG_FILE)
    
    if file_size >= FILE_SIZE_DELETE_THRESHOLD:
        send_to_discord(f"⚠️ **WARNING**: Log file has reached {file_size / (1024 * 1024 * 1024):.2f} GB. Clearing file content now.")
        # Clear file by opening in write mode
        with open(LOG_FILE, 'w'):
            pass
        return True
    elif file_size >= FILE_SIZE_WARNING_THRESHOLD:
        send_to_discord(f"⚠️ **WARNING**: Log file size is {file_size / (1024 * 1024 * 1024):.2f} GB. Consider manual deletion.")
    
    return False

def parse_timestamp(log_line):
    """Extract timestamp from log line and return datetime object."""
    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', log_line)
    if timestamp_match:
        timestamp_str = timestamp_match.group(1)
        try:
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    return None

def parse_and_send_logs(minutes=DEFAULT_MINUTES):
    if not os.path.exists(LOG_FILE):
        send_to_discord(f"Error: File {LOG_FILE} not found")
        return
        
    if os.path.getsize(LOG_FILE) == 0:
        send_to_discord(f"Warning: {LOG_FILE} is empty. Nothing to process.")
        return
    
    if not DISCORD_WEBHOOK_URL:
        raise ValueError("DISCORD_WEBHOOK_URL environment variable is not set.")

    # Check file size first
    file_cleared = check_file_size()
    if file_cleared:
        send_to_discord("Log file was cleared due to size threshold. Exiting.")
        return

    logs_by_pattern = {pattern: [] for pattern in PATTERNS}
    
    current_time_utc = time.time() 
    current_time_utc_datetime = datetime.fromtimestamp(current_time_utc)
    cutoff_time = current_time_utc_datetime - timedelta(minutes=minutes)
    print(cutoff_time)
    print(current_time_utc_datetime)

    try:
        with open(LOG_FILE, 'r') as f:
            for line in f:
                # Extract timestamp and compare with cutoff time
                timestamp = parse_timestamp(line)
                if timestamp and timestamp >= cutoff_time:
                    for pattern_name, pattern in PATTERNS.items():
                        if re.search(pattern, line):
                            logs_by_pattern[pattern_name].append(line.strip())
                            break 
    except Exception as e:
        raise ValueError(f"Error reading log file: {e}") from e

    total_logs = sum(len(logs) for logs in logs_by_pattern.values())
    if total_logs == 0:
        send_to_discord(f"No matching logs found in the last {minutes} minutes")
        return
    
    summary = {
        "total_matched_logs": total_logs,
        "counts": {pattern: len(logs) for pattern, logs in logs_by_pattern.items()},
        "time_range": f"Last {minutes} minutes (since {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} UTC)"
    }
    
    send_to_discord(f"**Log Summary from the last {minutes} minutes**\n```json\n" + json.dumps(summary, indent=2) + "\n```")
    
    for pattern, logs in logs_by_pattern.items():
            
        send_to_discord(f"**{pattern} Logs** ({len(logs)} entries)")
        
        # Send logs in batches
        for i in range(0, min(len(logs), MAX_LOGS_TO_SEND), BATCH_SIZE):
            if len(logs) >= MAX_LOGS_TO_SEND:
                send_to_discord(f"Cannot send more than **{MAX_LOGS_TO_SEND} {pattern} Logs** Please check the logs...")
                break

            batch = logs[i:i + BATCH_SIZE]
            message = "\n".join(batch)
            
            # Truncate if too long (Discord limit: 1000 characters)
            if len(message) > 1000:
                message = message[:950] + "\n... (truncated)"
                
            send_to_discord(f"```\n{message}\n```")
    
def send_to_discord(message):
    """Send a single message to Discord webhook"""
    
    _random_sleep()

    payload = {
        "content": message,
        "username": "Log Bot"
    }
    
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Failed to send message: {e}") from e

if __name__ == "__main__":
    parse_and_send_logs()