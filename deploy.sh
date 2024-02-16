#!/bin/bash

poetry export -f requirements.txt --output requirements.txt --without-hashes

# Convert .env file to .env.yaml by removing GOOGLE_APPLICATION_CREDENTIALS and replacing '=' with ':', adding double quotes around values
awk -F= '{print $1 ": \"" $2 "\""}' .env | grep -v GOOGLE_APPLICATION_CREDENTIALS >.env.yaml

PROJECT_ID="vitaminb16"
CLOUD_FUNCTION_NAME="python-roh"
REGION="europe-west2"
ENTRY_POINT="entry_point"
RUNTIME="python310"

# Deploy the Cloud Function
gcloud functions deploy $CLOUD_FUNCTION_NAME \
  --project=$PROJECT_ID \
  --runtime=$RUNTIME \
  --trigger-http \
  --entry-point=$ENTRY_POINT \
  --region=$REGION \
  --env-vars-file .env.yaml \
  --timeout=120s \
  --memory=512MB
