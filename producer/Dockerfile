# Use a lightweight Python image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy dependency file and install libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your producer script into the image
COPY producer.py .

# Run the script automatically when the container starts
CMD ["python", "producer.py"]
