# Neptune Java Demo

A Java Maven project demonstrating how to connect to Amazon Neptune using OpenCypher queries via both Bolt protocol and REST API.

## Key Features

- **Dual Connection Methods**: 
  - `NeptuneBoltDemo`: Uses Bolt driver with Bolt protocol for OpenCypher queries
  - `NeptuneDataApiDemo`: Uses AWS SDK Neptune Data API for REST-based OpenCypher queries
- **OpenCypher Support**: Execute graph queries using OpenCypher syntax
- **Configuration Management**: Environment variables and properties file support
- **Automated Deployment**: Deploy script for easy EC2 deployment
- **Log4j 2**: Modern logging framework with SLF4J bridge for Bolt driver compatibility

## Prerequisites

### Local Development Environment
- Java 11 or higher
- Maven 3.6 or higher
- SSH access to your EC2 instance

### EC2 Instance Requirements
- Amazon Linux 2, Ubuntu 18.04+, or CentOS 7+
- Java 11 or higher (will be installed automatically)
- Network connectivity to your Neptune cluster
- SSH access configured

### Neptune Cluster
- Neptune cluster
- Security groups configured to allow access from EC2 instance
- Cluster endpoint and port information

## Usage

### Running the Application

The application provides a unified entry point that can run either demo based on a command-line parameter:

```bash
# Run with Bolt driver (Bolt protocol)
java -jar target/neptune-demo-app.jar bolt

# Run with Neptune Data API (REST)
java -jar target/neptune-demo-app.jar data-api
```

### Available Demo Types

- **bolt**: Uses Bolt driver with Bolt protocol for OpenCypher queries
- **data-api**: Uses AWS SDK Neptune Data API for REST-based OpenCypher queries

## Deploy

### Configuration

The application supports multiple configuration methods with a clear priority hierarchy:

**Priority Order (highest to lowest):**
1. Environment variables
2. `application.properties` file
3. Default values

**Local Development - application.properties**
```properties
# Neptune Configuration
neptune.endpoint=your-cluster-endpoint.amazonaws.com
neptune.port=8182

# AWS Configuration  
aws.region=us-east-1

# IAM Authentication
neptune.iam.auth=false

# AWS Credentials (optional)
aws.access.key=your-access-key
aws.secret.key=your-secret-key
aws.session.token=your-session-token
```

**Environment Variables**
```bash
export NEPTUNE_ENDPOINT="your-cluster-endpoint.amazonaws.com"
export NEPTUNE_PORT="8182"
export AWS_REGION="us-east-1"
export NEPTUNE_IAM_AUTH="true"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_SESSION_TOKEN="your-session-token"
```

**Deploy Script Configuration**

The `./deploy.sh configure` command provides an interactive setup:
- Prompts for Neptune endpoint and port
- Asks for AWS region
- Enables/disables IAM authentication
- Optionally collects AWS credentials (Access Key, Secret Key, Session Token)
- Creates environment configuration file on EC2 instance

### IAM Authentication

**Disabled (default):** Uses no authentication - suitable for Neptune clusters in private VPCs.

**Enabled:** Uses AWS IAM for authentication with these credential options:
- **Default Credential Chain:** Leave credentials empty to use EC2 instance roles, AWS profiles, or environment
- **Static Credentials:** Provide Access Key + Secret Key for long-term credentials  
- **Temporary Credentials:** Provide Access Key + Secret Key + Session Token for STS/assumed role credentials

When IAM auth is enabled:
- **Bolt Demo:** Uses `NeptuneAuthToken` with AWS Signature Version 4
- **Data API Demo:** Uses AWS SDK credential providers automatically

### Quick Deployment with Deploy Script

```bash
# Deploy to EC2 instance
./deploy.sh deploy --host your-ec2-instance.amazonaws.com --key ~/.ssh/your-key.pem

# Configure Neptune connection (this is necessary only once)
./deploy.sh configure --host your-ec2-instance.amazonaws.com --key ~/.ssh/your-key.pem

# Run the application with different demo types
./deploy.sh run --host your-ec2-instance.amazonaws.com --key ~/.ssh/your-key.pem                    # Bolt demo (default)
./deploy.sh run --host your-ec2-instance.amazonaws.com --key ~/.ssh/your-key.pem --demo-type bolt  # Bolt demo
./deploy.sh run --host your-ec2-instance.amazonaws.com --key ~/.ssh/your-key.pem --demo-type data-api # Data API demo
```

Use `./deploy.sh --help` for all available commands.

## Resources

- [Amazon Neptune User Guide](https://docs.aws.amazon.com/neptune/latest/userguide/)
- [OpenCypher Query Language](https://opencypher.org/)
