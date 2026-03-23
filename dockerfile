# Use Python base image
FROM python:3.11-slim

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install pip and virtualenv
RUN pip install --upgrade pip
RUN pip install virtualenv

# Create folders for each component
RUN mkdir -p /app/dbt /app/spark

# Set Working Directory for dbt and install dbt dependencies
WORKDIR /app/dbt
RUN python3 -m venv /app/dbt/venv
RUN /app/dbt/venv/bin/pip install dbt-bigquery

# Set Working Directory for Spark and install Spark dependencies
WORKDIR /app/spark
RUN python3 -m venv /app/spark/venv
RUN /app/spark/venv/bin/pip install pyspark

# Copy project files into the container
COPY . /app

# Expose a different port (5000 in this case)
EXPOSE 5000

# Run container with bash interactive or any required scripts
CMD ["/bin/bash"]