# Build the main worker image
FROM ubuntu:22.04

# Set up environment
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary packages
RUN apt-get update && apt-get install -y \
    dnsutils \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copy script and set working directory
COPY py /app/py
COPY lat-long.csv /app/lat-long.csv
WORKDIR /app

# Authenticate with Whoop
CMD ["python3", "py/engine.py"]