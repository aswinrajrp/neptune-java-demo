#!/bin/bash

# Neptune Java Demo - Application Status Script
# This script checks the status of the Neptune demo application on EC2

set -e

# Configuration
APP_NAME="Neptune Java Demo"
PID_FILE="neptune-demo.pid"
LOG_FILE="neptune-demo.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}$APP_NAME Status Check${NC}"
echo "================================"

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo -e "${RED}Status: STOPPED${NC}"
    echo "PID file not found"
    exit 0
fi

# Read PID from file
PID=$(cat "$PID_FILE")

# Check if process is running
if ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${GREEN}Status: RUNNING${NC}"
    echo "PID: $PID"
    
    # Get process information
    echo "Process Info:"
    ps -p "$PID" -o pid,ppid,cmd,etime,pcpu,pmem --no-headers | while read line; do
        echo "  $line"
    done
    
    # Check log file
    if [ -f "$LOG_FILE" ]; then
        echo ""
        echo "Log File: $LOG_FILE"
        echo "Log Size: $(du -h "$LOG_FILE" | cut -f1)"
        echo "Last Modified: $(stat -c %y "$LOG_FILE" 2>/dev/null || stat -f %Sm "$LOG_FILE" 2>/dev/null || echo "Unknown")"
        
        echo ""
        echo "Recent Log Entries (last 10 lines):"
        echo "-----------------------------------"
        tail -n 10 "$LOG_FILE" | sed 's/^/  /'
    else
        echo -e "${YELLOW}Log file not found: $LOG_FILE${NC}"
    fi
    
else
    echo -e "${RED}Status: STOPPED${NC}"
    echo "Process with PID $PID is not running"
    echo "Removing stale PID file"
    rm -f "$PID_FILE"
fi

echo ""
echo "Available Commands:"
echo "  ./run-app.sh    - Start the application"
echo "  ./stop-app.sh   - Stop the application"
echo "  ./status-app.sh - Check application status"
