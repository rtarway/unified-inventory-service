#!/bin/bash

# Master Deployment Script for Unified Inventory System & Stream Processor
# Assumes:
# 1. Running from 'unified-inventory-service' root or containing folder.
# 2. 'inventory-stream-processor' is a sibling directory.
# 3. mvn, docker, and kubectl are installed and configured.

set -e # Exit on error

echo "=================================================="
echo "Starting Master Deployment"
echo "=================================================="

# Check Sibling Directory
if [ ! -d "../inventory-stream-processor" ]; then
    echo "❌ Error: ../inventory-stream-processor not found!"
    exit 1
fi

echo "[1/5] Building Stream Processor (Java)..."
pushd ../inventory-stream-processor
# Build JARs
mvn clean package -DskipTests

# Build Docker Images
echo "      Building Docker Images..."
docker build -t inventory-api:latest api/
docker build -t inventory-processor:latest processor/
docker build -t inventory-sink:latest sink/
popd

echo "[2/5] Building Unified Inventory Service (Node)..."
# Build Docker Image
docker build -t unified-inventory-service:latest .

echo "[3/5] Deploying Infrastructure (Kafka, Redis, Postgres)..."
kubectl apply -f ../inventory-stream-processor/k8s/infrastructure.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/postgres.yaml

echo "      Waiting 10s for Infra to initialize..."
sleep 10

echo "[4/5] Deploying Stream Processor Apps..."
kubectl apply -f ../inventory-stream-processor/k8s/apps.yaml

echo "[5/5] Deploying Unified Inventory Service..."
kubectl apply -f k8s/config.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/cronjob.yaml

echo "=================================================="
echo "✅ Deployment Manifests Applied."
echo "=================================================="
echo "Next Steps:"
echo "1. Watch pods: 'kubectl get pods -w'"
echo "2. Once running, port-forward Unified Service:"
echo "   'kubectl port-forward svc/unified-inventory-service 3000:80'"
echo "3. Run E2E Test:"
echo "   './tests/e2e-k8s.sh'"
