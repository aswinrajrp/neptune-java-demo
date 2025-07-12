#!/bin/bash

# Neptune Java Demo - Application Stop Script
# This script stops the Neptune demo application on EC2

set -e

# Configuration
APP_NAME="Neptune Java Demo"
PID_FILE="neptune-demo.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Stopping $APP_NAME...${NC}"

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo -e "${YELLOW}PID file not found. Application may not be running.${NC}"
    exit 0
fi

# Read PID from file
PID=$(cat "$PID_FILE")

# Check if process is running
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${YELLOW}Process with PID $PID is not running${NC}"
    rm -f "$PID_FILE"
    exit 0
fi

# Try graceful shutdown first
echo -e "${YELLOW}Sending TERM signal to process $PID${NC}"
kill -TERM "$PID"

# Wait for graceful shutdown
TIMEOUT=30
COUNTER=0
while ps -p "$PID" > /dev/null 2>&1 && [ $COUNTER -lt $TIMEOUT ]; do
    sleep 1
    COUNTER=$((COUNTER + 1))
    echo -n "."
done
echo

# Check if process stopped gracefully
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${GREEN}Application stopped gracefully${NC}"
    rm -f "$PID_FILE"
    exit 0
fi

# Force kill if graceful shutdown failed
echo -e "${YELLOW}Graceful shutdown failed. Force killing process...${NC}"
kill -KILL "$PID"

# Wait a moment and verify
sleep 2
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${GREEN}Application force stopped${NC}"
    rm -f "$PID_FILE"
else
    echo -e "${RED}Failed to stop application${NC}"
    exit 1
fi
