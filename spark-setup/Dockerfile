# Use official Python image
FROM python:3.10-slim

# Install Java (needed by Spark) and curl (for downloading jars)
RUN apt-get update && apt-get install -y default-jdk python3-tk curl && apt-get clean

# Set working directory
WORKDIR /app

# Copy dependency list and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Spark Kafka connector and its dependencies into PySpark's JAR directory
RUN mkdir -p /usr/local/lib/python3.10/site-packages/pyspark/jars && \
    curl -L -o /usr/local/lib/python3.10/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /usr/local/lib/python3.10/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /usr/local/lib/python3.10/site-packages/pyspark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o /usr/local/lib/python3.10/site-packages/pyspark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar


# Copy your consumer script
COPY consumer.py .

# Expose Spark UI port (optional)
EXPOSE 4040

# Run the Spark consumer
CMD ["python", "consumer.py"]
