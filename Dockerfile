# Use an official Python runtime as a base image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the necessary files
COPY scripts/cron.py .
COPY requirements.txt .

# Install dos2unix
RUN apt-get update \
    && apt-get -y install dos2unix

# Convert line endings and ensure the script is executable
RUN dos2unix cron.py \
    && chmod +x cron.py

# Install cron
RUN apt-get -y install cron

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Start cron service
CMD ["cron", "-f"]
