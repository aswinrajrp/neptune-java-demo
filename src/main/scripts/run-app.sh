#!/bin/bash

# Neptune Java Demo - Application Runner Script
# This script runs the Neptune demo application on EC2

set -e

# Configuration
APP_NAME="Neptune Java Demo"
JAR_FILE="neptune-demo-app.jar"
LOG_FILE="neptune-demo.log"
PID_FILE="neptune-demo.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo "Usage: $0 [demo-type]"
    echo ""
    echo "Demo Types:"
    echo "  bolt      - Run Neptune demo using Bolt driver with Bolt protocol (default)"
    echo "  data-api  - Run Neptune demo using AWS SDK Neptune Data API (REST)"
    echo ""
    echo "Examples:"
    echo "  $0              # Run with Bolt driver (default)"
    echo "  $0 bolt         # Run with Bolt driver"
    echo "  $0 data-api     # Run with Data API"
}

# Parse command line arguments
DEMO_TYPE="bolt"  # Default to bolt
if [ $# -gt 1 ]; then
    echo -e "${RED}Error: Too many arguments${NC}"
    show_usage
    exit 1
elif [ $# -eq 1 ]; then
    if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
        show_usage
        exit 0
    fi
    DEMO_TYPE="$1"
fi

# Validate demo type
if [ "$DEMO_TYPE" != "bolt" ] && [ "$DEMO_TYPE" != "data-api" ]; then
    echo -e "${RED}Error: Invalid demo type '$DEMO_TYPE'${NC}"
    echo "Valid options are: bolt, data-api"
    echo ""
    show_usage
    exit 1
fi

echo -e "${GREEN}Starting $APP_NAME with $DEMO_TYPE demo...${NC}"

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo -e "${RED}Error: Java is not installed or not in PATH${NC}"
    echo "Please install Java 11 or higher"
    exit 1
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 11 ]; then
    echo -e "${RED}Error: Java 11 or higher is required${NC}"
    echo "Current Java version: $(java -version 2>&1 | head -n 1)"
    exit 1
fi

# Check if JAR file exists
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${RED}Error: $JAR_FILE not found${NC}"
    echo "Please ensure the application is properly deployed"
    exit 1
fi

# Load environment variables from config file if it exists
if [ -f "neptune-config.env" ]; then
    echo -e "${YELLOW}Loading configuration from neptune-config.env${NC}"
    source neptune-config.env
fi

# Set default values if environment variables are not set
export NEPTUNE_ENDPOINT=${NEPTUNE_ENDPOINT:-"your-neptune-cluster-endpoint.cluster-xxxxxxxxx.us-east-1.neptune.amazonaws.com"}
export NEPTUNE_PORT=${NEPTUNE_PORT:-"8182"}

# Validate Neptune configuration
if [ "$NEPTUNE_ENDPOINT" = "your-neptune-cluster-endpoint.cluster-xxxxxxxxx.us-east-1.neptune.amazonaws.com" ]; then
    echo -e "${YELLOW}Warning: Using default Neptune endpoint. Please configure NEPTUNE_ENDPOINT environment variable.${NC}"
fi

echo -e "${GREEN}Configuration:${NC}"
echo "  Demo Type: $DEMO_TYPE"
echo "  Neptune Endpoint: $NEPTUNE_ENDPOINT"
echo "  Neptune Port: $NEPTUNE_PORT"
echo "  Log File: $LOG_FILE"

# Check if application is already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo -e "${YELLOW}Application is already running with PID: $PID${NC}"
        echo "Use stop-app.sh to stop the application first"
        exit 1
    else
        echo -e "${YELLOW}Removing stale PID file${NC}"
        rm -f "$PID_FILE"
    fi
fi

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}Shutting down application...${NC}"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            # Try graceful shutdown first
            kill "$PID" 2>/dev/null
            
            # Wait up to 5 seconds for graceful shutdown
            for i in {1..5}; do
                if ! ps -p "$PID" > /dev/null 2>&1; then
                    echo -e "${GREEN}Application stopped gracefully${NC}"
                    break
                fi
                sleep 1
            done
            
            # Force kill if still running
            if ps -p "$PID" > /dev/null 2>&1; then
                echo -e "${YELLOW}Force killing application...${NC}"
                kill -9 "$PID" 2>/dev/null
                sleep 1
                if ps -p "$PID" > /dev/null 2>&1; then
                    echo -e "${RED}Failed to stop application${NC}"
                else
                    echo -e "${GREEN}Application force stopped${NC}"
                fi
            fi
        fi
        rm -f "$PID_FILE"
    fi
    
    # Also kill the tail process if it's still running
    if [ -n "$TAIL_PID" ] && ps -p "$TAIL_PID" > /dev/null 2>&1; then
        kill "$TAIL_PID" 2>/dev/null
    fi
}

# Set trap to cleanup on script exit
trap cleanup EXIT INT TERM

# Run the application
echo -e "${GREEN}Starting application...${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the application${NC}"
echo "----------------------------------------"

# Start the application in background and capture PID
java -jar "$JAR_FILE" "$DEMO_TYPE" > "$LOG_FILE" 2>&1 &
APP_PID=$!

# Save PID to file
echo "$APP_PID" > "$PID_FILE"
echo -e "${GREEN}Application started with PID: $APP_PID${NC}"
echo "Tailing logs (Ctrl+C to stop):"
echo "----------------------------------------"

# Tail the log file and monitor the process
tail -f "$LOG_FILE" &
TAIL_PID=$!

# Monitor the application process
while ps -p "$APP_PID" > /dev/null 2>&1; do
    sleep 1
done

# If we get here, the application process has died
echo -e "\n${GREEN}Application process has terminated${NC}"

# Kill the tail process
if [ -n "$TAIL_PID" ] && ps -p "$TAIL_PID" > /dev/null 2>&1; then
    kill "$TAIL_PID" 2>/dev/null
fi

exit 0
