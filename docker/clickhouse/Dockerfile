FROM clickhouse/clickhouse-server:latest

# Install required packages for Kafka integration
USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy custom configuration files
COPY config.xml /etc/clickhouse-server/config.xml
COPY users.xml /etc/clickhouse-server/users.xml

# Copy initialization scripts
COPY init/ /docker-entrypoint-initdb.d/

# Set proper permissions
RUN chown -R clickhouse:clickhouse /etc/clickhouse-server/ && \
    chown -R clickhouse:clickhouse /docker-entrypoint-initdb.d/ && \
    chmod 644 /etc/clickhouse-server/users.xml

USER clickhouse

# Expose ports
EXPOSE 8123 9000 9009
