#!/bin/bash

# Start a new Minikube cluster
echo "Starting a new Minikube cluster..."
minikube start

# Build the Docker image
echo "Building the Docker image..."
docker build -t travops87/officiallist:latest .

# Push the Docker image to Docker Hub
echo "Pushing the Docker image to Docker Hub..."
docker push travops87/officiallist:latest

# Install the Helm chart
echo "Installing the Helm chart..."
helm install official-list ./helm --namespace official-list --create-namespace

echo "Add namespace to context"
kubens official-list

echo "Script execution completed."