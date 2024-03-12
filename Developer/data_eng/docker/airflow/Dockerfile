# Base Image
FROM python:3.8-slim-buster as builder

LABEL maintainer="Shaunjay"

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=2.7.3

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}
ENV VENV_PATH="/opt/venv"
ENV PATH="${VENV_PATH}/bin:$PATH"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Install OS dependencies
RUN apt-get update -y && \
    apt-get install -yqq --no-install-recommends \
    python3-dev wget curl libssl-dev git inetutils-telnet \
    bind9utils freetds-dev libkrb5-dev libsasl2-dev \
    libffi-dev libpq-dev freetds-bin build-essential default-libmysqlclient-dev gcc vim netcat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a virtual environment
RUN python3 -m venv ${VENV_PATH}

# Upgrade pip and install apache-airflow with the required extras
RUN . ${VENV_PATH}/bin/activate && \
    pip install --upgrade pip setuptools wheel && \
    pip install apache-airflow==${AIRFLOW_VERSION} \
                 'apache-airflow-providers-amazon' \
                 psycopg2-binary \
                 'apache-airflow-providers-snowflake' \
                 'apache-airflow-providers-http' \
                 boto3 \
                 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt

# Stage 2: Runtime environment
FROM python:3.8-slim-buster

# Create user and group 'airflow'
RUN groupadd airflow && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} -g airflow airflow

# Copy virtual environment from builder
COPY --from=builder ${VENV_PATH} ${VENV_PATH}

# Copy the start-airflow.sh script and Python script to the container's /opt/airflow directory
COPY ./start-air
