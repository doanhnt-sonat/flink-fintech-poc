# Use the official Flink image with Python support
FROM flink:2.0.0
RUN mkdir -p /opt/flink/usrlib
RUN wget -P /opt/flink/usrlib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar
# Switch to root user to install packages
USER root

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic link for python
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Set working directory
WORKDIR /opt/flink

# Copy requirements and install Python dependencies
COPY flink-jobs-python/requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Copy Python jobs
COPY flink-jobs-python/src /opt/flink/python-jobs

# Set Python path
ENV PYTHONPATH="/opt/flink/python-jobs:$PYTHONPATH"

# Switch back to flink user
USER flink

# Default command (can be overridden)
CMD ["help"]
