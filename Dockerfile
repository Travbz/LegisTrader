# Use an official Python runtime as a base image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the necessary files
COPY requirements.txt /app/
COPY scripts/ /app/scripts/
COPY k8s/ /app/k8s/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run your script when the container launches
CMD ["python", "./scripts/cron.py"]