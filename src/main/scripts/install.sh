#!/bin/bash

# Neptune Java Demo - Installation Script for EC2
# This script sets up the environment and installs dependencies

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Neptune Java Demo - EC2 Installation${NC}"
echo "===================================="

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    echo -e "${YELLOW}Warning: Running as root. Consider using a non-root user.${NC}"
fi

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$NAME
    VER=$VERSION_ID
else
    echo -e "${RED}Cannot detect OS version${NC}"
    exit 1
fi

echo -e "${GREEN}Detected OS: $OS $VER${NC}"

# Install Java if not present
if ! command -v java &> /dev/null; then
    echo -e "${YELLOW}Java not found. Installing OpenJDK 11...${NC}"
    
    if [[ "$OS" == *"Amazon Linux"* ]]; then
        sudo yum update -y
        sudo yum install -y java-11-openjdk java-11-openjdk-devel
    elif [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        sudo apt-get update
        sudo apt-get install -y openjdk-11-jdk
    elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]]; then
        sudo yum update -y
        sudo yum install -y java-11-openjdk java-11-openjdk-devel
    else
        echo -e "${RED}Unsupported OS for automatic Java installation${NC}"
        echo "Please install Java 11 manually"
        exit 1
    fi
else
    echo -e "${GREEN}Java is already installed${NC}"
fi

# Verify Java version
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 11 ]; then
    echo -e "${RED}Error: Java 11 or higher is required${NC}"
    echo "Current Java version: $(java -version 2>&1 | head -n 1)"
    exit 1
fi

echo -e "${GREEN}Java version check passed${NC}"
java -version

# Create application directory
APP_DIR="/opt/neptune-demo"
echo -e "${YELLOW}Creating application directory: $APP_DIR${NC}"
sudo mkdir -p "$APP_DIR"
sudo chown $(whoami):$(whoami) "$APP_DIR" 2>/dev/null || sudo chown $(whoami) "$APP_DIR"

# Create systemd service file (optional)
echo -e "${YELLOW}Creating systemd service file...${NC}"
sudo tee /etc/systemd/system/neptune-demo.service > /dev/null <<EOF
[Unit]
Description=Neptune Java Demo Application
After=network.target

[Service]
Type=forking
User=$(whoami)
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/run-app.sh
ExecStop=$APP_DIR/stop-app.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
sudo systemctl daemon-reload

echo -e "${GREEN}Installation completed successfully!${NC}"
echo ""
echo "Next steps:"
echo "1. Deploy your application to $APP_DIR"
echo "2. Configure Neptune connection in neptune-config.env"
echo "3. Run the application with ./run-app.sh"
echo ""
echo "Optional: Enable systemd service:"
echo "  sudo systemctl enable neptune-demo"
echo "  sudo systemctl start neptune-demo"
