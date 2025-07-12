#!/bin/bash

# Neptune Java Demo - Deployment Helper Script
# Quick commands for common deployment tasks

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
EC2_HOST=""
EC2_USER="ec2-user"
EC2_KEY=""
DEMO_TYPE=""

# Function to show usage
show_usage() {
    echo -e "${BLUE}Neptune Java Demo - Deployment Helper${NC}"
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  build                 - Build the application locally"
    echo "  package              - Create deployment package"
    echo "  deploy               - Deploy to EC2 instance"
    echo "  configure            - Configure Neptune connection on EC2"
    echo "  run                  - Run application on EC2"
    echo "    --demo-type TYPE   - Demo type: bolt (default) or data-api"
    echo "  status               - Check application status on EC2"
    echo "  logs                 - View application logs on EC2"
    echo "  stop                 - Stop application on EC2"
    echo "  install              - Install dependencies on EC2"
    echo ""
    echo "Options:"
    echo "  --host HOST          - EC2 instance hostname or IP"
    echo "  --user USER          - SSH user (default: ec2-user)"
    echo "  --key PATH           - Path to SSH private key"
    echo "  --demo-type TYPE     - Demo type for run command: bolt or data-api (default: bolt)"
    echo ""
    echo "Examples:"
    echo "  $0 build"
    echo "  $0 deploy --host my-ec2.amazonaws.com --key ~/.ssh/my-key.pem"
    echo "  $0 configure --host my-ec2.amazonaws.com --key ~/.ssh/my-key.pem"
    echo "  $0 run --host my-ec2.amazonaws.com --key ~/.ssh/my-key.pem"
    echo "  $0 run --host my-ec2.amazonaws.com --key ~/.ssh/my-key.pem --demo-type bolt"
    echo "  $0 run --host my-ec2.amazonaws.com --key ~/.ssh/my-key.pem --demo-type data-api"
}

# Parse command line arguments
COMMAND=""
while [[ $# -gt 0 ]]; do
    case $1 in
        build|package|deploy|configure|run|status|logs|stop|install)
            COMMAND="$1"
            shift
            ;;
        --host)
            EC2_HOST="$2"
            shift 2
            ;;
        --user)
            EC2_USER="$2"
            shift 2
            ;;
        --key)
            EC2_KEY="$2"
            shift 2
            ;;
        --demo-type)
            DEMO_TYPE="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_usage
            exit 1
            ;;
    esac
done

# Validate command
if [ -z "$COMMAND" ]; then
    echo -e "${RED}Error: No command specified${NC}"
    show_usage
    exit 1
fi

# SSH command builder
build_ssh_cmd() {
    if [ -z "$EC2_HOST" ]; then
        echo -e "${RED}Error: EC2 host not specified${NC}"
        exit 1
    fi
    
    SSH_CMD="ssh"
    if [ -n "$EC2_KEY" ]; then
        SSH_CMD="$SSH_CMD -i $EC2_KEY"
    fi
    SSH_CMD="$SSH_CMD $EC2_USER@$EC2_HOST"
}

# SCP command builder
build_scp_cmd() {
    if [ -z "$EC2_HOST" ]; then
        echo -e "${RED}Error: EC2 host not specified${NC}"
        exit 1
    fi
    
    SCP_CMD="scp"
    if [ -n "$EC2_KEY" ]; then
        SCP_CMD="$SCP_CMD -i $EC2_KEY"
    fi
}

# Execute commands based on input
case $COMMAND in
    build)
        echo -e "${GREEN}Building application...${NC}"
        mvn clean compile
        echo -e "${GREEN}Build completed${NC}"
        ;;
        
    package)
        echo -e "${GREEN}Creating deployment package...${NC}"
        mvn clean package
        echo -e "${GREEN}Package created: target/neptune-demo-deployment.tar.gz${NC}"
        ;;
        
    deploy)
        build_ssh_cmd
        build_scp_cmd
        
        echo -e "${GREEN}Building and deploying to $EC2_HOST...${NC}"
        
        # Build package
        mvn clean package
        
        # Create directory on EC2
        echo -e "${YELLOW}Creating deployment directory...${NC}"
        $SSH_CMD "sudo mkdir -p /opt/neptune-demo && sudo chown \$(whoami) /opt/neptune-demo"
        
        # Copy deployment package
        echo -e "${YELLOW}Copying deployment package...${NC}"
        $SCP_CMD target/neptune-demo-deployment.tar.gz $EC2_USER@$EC2_HOST:/opt/neptune-demo/
        
        # Extract and setup
        echo -e "${YELLOW}Extracting and setting up...${NC}"
        $SSH_CMD "cd /opt/neptune-demo && tar -xzf neptune-demo-deployment.tar.gz && chmod +x *.sh"
        
        echo -e "${GREEN}Deployment completed successfully${NC}"
        echo "Next steps:"
        echo "1. Configure Neptune connection: $0 configure --host $EC2_HOST --key $EC2_KEY"
        echo "2. Run the application:"
        echo "   $0 run --host $EC2_HOST --key $EC2_KEY                    # Run Bolt demo (default)"
        echo "   $0 run --host $EC2_HOST --key $EC2_KEY --demo-type bolt  # Run Bolt demo"
        echo "   $0 run --host $EC2_HOST --key $EC2_KEY --demo-type data-api # Run Data API demo"
        ;;
        
    configure)
        build_ssh_cmd
        build_scp_cmd
        
        echo -e "${GREEN}Configuring Neptune connection on $EC2_HOST...${NC}"
        
        # Prompt for Neptune configuration
        echo -e "${YELLOW}Please provide your Neptune cluster details:${NC}"
        read -p "Neptune Endpoint: " NEPTUNE_ENDPOINT
        read -p "Neptune Port (default: 8182): " NEPTUNE_PORT
        NEPTUNE_PORT=${NEPTUNE_PORT:-8182}
        read -p "AWS Region (e.g., us-east-1): " AWS_REGION
        
        # Ask about IAM Auth
        echo ""
        read -p "Enable IAM Authentication? (y/n, default: n): " IAM_AUTH_INPUT
        IAM_AUTH_INPUT=${IAM_AUTH_INPUT:-n}
        
        if [[ "$IAM_AUTH_INPUT" =~ ^[Yy]$ ]]; then
            IAM_AUTH="true"
            echo -e "${YELLOW}IAM Auth enabled. You can optionally provide AWS credentials:${NC}"
            read -p "AWS Access Key ID (optional, leave empty to use default credentials): " AWS_ACCESS_KEY
            read -p "AWS Secret Access Key (optional, leave empty to use default credentials): " AWS_SECRET_KEY
            read -p "AWS Session Token (optional, for temporary credentials): " AWS_SESSION_TOKEN
        else
            IAM_AUTH="false"
            AWS_ACCESS_KEY=""
            AWS_SECRET_KEY=""
            AWS_SESSION_TOKEN=""
        fi
        
        # Create configuration file locally
        cat > /tmp/neptune-config.env << EOF
# Neptune Java Demo - Environment Configuration
# Generated by deploy.sh configure command

# Neptune Cluster Configuration
export NEPTUNE_ENDPOINT="$NEPTUNE_ENDPOINT"
export NEPTUNE_PORT="$NEPTUNE_PORT"

# AWS Configuration
export AWS_REGION="$AWS_REGION"

# IAM Authentication
export NEPTUNE_IAM_AUTH="$IAM_AUTH"
EOF

        # Add AWS credentials if provided
        if [ -n "$AWS_ACCESS_KEY" ]; then
            echo "export AWS_ACCESS_KEY_ID=\"$AWS_ACCESS_KEY\"" >> /tmp/neptune-config.env
        fi
        
        if [ -n "$AWS_SECRET_KEY" ]; then
            echo "export AWS_SECRET_ACCESS_KEY=\"$AWS_SECRET_KEY\"" >> /tmp/neptune-config.env
        fi
        
        if [ -n "$AWS_SESSION_TOKEN" ]; then
            echo "export AWS_SESSION_TOKEN=\"$AWS_SESSION_TOKEN\"" >> /tmp/neptune-config.env
        fi
        
        # Add remaining configuration
        cat >> /tmp/neptune-config.env << EOF

# Java Options
export JAVA_OPTS="-Xmx512m -Xms256m"

# Application Configuration
export LOG_LEVEL="INFO"
EOF
        
        # Copy configuration to EC2
        echo -e "${YELLOW}Uploading configuration to $EC2_HOST...${NC}"
        $SCP_CMD /tmp/neptune-config.env $EC2_USER@$EC2_HOST:/opt/neptune-demo/
        
        # Clean up temporary file
        rm -f /tmp/neptune-config.env
        
        echo -e "${GREEN}Configuration completed successfully${NC}"
        echo "Configuration files updated:"
        echo "  - /opt/neptune-demo/neptune-config.env"
        echo ""
        echo "Next step: Run the application with:"
        echo "  $0 run --host $EC2_HOST --key $EC2_KEY                      # Run Bolt demo (default)"
        echo "  $0 run --host $EC2_HOST --key $EC2_KEY --demo-type bolt     # Run Bolt demo"
        echo "  $0 run --host $EC2_HOST --key $EC2_KEY --demo-type data-api # Run Data API demo"
        ;;
        
    run)
        build_ssh_cmd
        
        # Set default demo type if not specified
        if [ -z "$DEMO_TYPE" ]; then
            DEMO_TYPE="bolt"
        fi
        
        # Validate demo type
        if [ "$DEMO_TYPE" != "bolt" ] && [ "$DEMO_TYPE" != "data-api" ]; then
            echo -e "${RED}Error: Invalid demo type '$DEMO_TYPE'${NC}"
            echo "Valid options are: bolt, data-api"
            exit 1
        fi
        
        echo -e "${GREEN}Starting $DEMO_TYPE demo on $EC2_HOST...${NC}"
        $SSH_CMD "cd /opt/neptune-demo && ./run-app.sh $DEMO_TYPE"
        ;;
        
    status)
        build_ssh_cmd
        echo -e "${GREEN}Checking application status on $EC2_HOST...${NC}"
        $SSH_CMD "cd /opt/neptune-demo && ./status-app.sh"
        ;;
        
    logs)
        build_ssh_cmd
        echo -e "${GREEN}Viewing logs on $EC2_HOST...${NC}"
        $SSH_CMD "cd /opt/neptune-demo && tail -f neptune-demo.log"
        ;;
        
    stop)
        build_ssh_cmd
        echo -e "${GREEN}Stopping application on $EC2_HOST...${NC}"
        $SSH_CMD "cd /opt/neptune-demo && ./stop-app.sh"
        ;;
        
    install)
        build_ssh_cmd
        build_scp_cmd
        
        echo -e "${GREEN}Installing dependencies on $EC2_HOST...${NC}"
        
        # Copy installation script
        $SCP_CMD src/main/scripts/install.sh $EC2_USER@$EC2_HOST:~/
        
        # Run installation
        $SSH_CMD "chmod +x install.sh && ./install.sh"
        
        echo -e "${GREEN}Installation completed${NC}"
        ;;
        
    *)
        echo -e "${RED}Unknown command: $COMMAND${NC}"
        show_usage
        exit 1
        ;;
esac
