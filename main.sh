#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd $SCRIPT_DIR

# Create logs directory if it doesn't exist
mkdir -p "$SCRIPT_DIR/logs"

echo "Starting 'main.py' at $(date)" >> "$SCRIPT_DIR/logs/main_logger.log"

# Source your zshrc to ensure Poetry & PATH are set correctly
source /root/.zshrc

# Load environment variables
set -a
source "$SCRIPT_DIR/.env"
set +a

# Run the Python script and redirect its output to a log file
poetry run python "$SCRIPT_DIR/src/main.py" >> "$SCRIPT_DIR/logs/script_output.log" 2>&1

echo "Finished script at $(date)" >> "$SCRIPT_DIR/logs/main_logger.log"