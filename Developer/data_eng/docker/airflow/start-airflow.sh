#!/usr/bin/env bash

# Assuming you have AWS CLI tools installed on your Docker image

# Fetch secrets from AWS Parameter Store
AWS_ACCESS_KEY=$(aws ssm get-parameter --name "/airflow/aws_access_key" --with-decryption --query 'Parameter.Value' --output text)
AWS_SECRET_KEY=$(aws ssm get-parameter --name "/airflow/aws_secret_key" --with-decryption --query 'Parameter.Value' --output text)
SLACK_TOKEN=$(aws ssm get-parameter --name "/airflow/slack_token" --with-decryption --query 'Parameter.Value' --output text)

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Export environment variables
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initialise the metadatabase
airflow db init

# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Add AWS connection
airflow connections add 'aws_default' \
    --conn-type 'aws' \
    --conn-login "$AWS_ACCESS_KEY" \
    --conn-password "$AWS_SECRET_KEY"

# Add Slack connection
airflow connections add 'Slack_Connection' \
    --conn-type 'http' \
    --conn-host 'https://hooks.slack.com/services/' \
    --conn-password "$SLACK_TOKEN"

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in the foreground (for docker logs)
exec airflow webserver
