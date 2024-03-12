#!/usr/bin/env bash

# Run the Python script to fetch and set AWS SSM parameters
python /opt/airflow/fetch_ssm_parameters.py

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Export environment variables
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initialise the metadatabase
airflow db init

# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Check if the aws_default connection exists
CONNECTION_EXISTS=$(airflow connections get aws_default)

if [[ "$CONNECTION_EXISTS" == "Connection not found" ]]; then
    # Add AWS connection
    airflow connections add 'aws_default' \
        --conn-type 'aws' \
        --conn-login "$AWS_ACCESS_KEY" \
        --conn-password "$AWS_SECRET_KEY"
    
    echo "DEBUG: AWS_ACCESS_KEY = $AWS_ACCESS_KEY"
    echo "DEBUG: AWS_SECRET_KEY = $AWS_SECRET_KEY"
else
    echo "aws_default connection already exists. Skipping addition."
fi

# Add Slack connection
airflow connections add 'Slack_Connection' \
    --conn-type 'http' \
    --conn-host 'https://hooks.slack.com/services/' \
    --conn-password "$SLACK_TOKEN"

echo "DEBUG: SLACK_TOKEN = $SLACK_TOKEN"

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in the foreground (for docker logs)
exec airflow webserver
