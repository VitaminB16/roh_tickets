#!/bin/bash

poetry export -f requirements.txt --output requirements.txt --without-hashes

PROJECT_ID="vitaminb16"
CLOUD_FUNCTION_NAME="python-roh"
REGION="europe-west2"
ENTRY_POINT="entry_point"
RUNTIME="python310"
SECRET_NAME=".env"

# Deploy the Cloud Function
gcloud functions deploy $CLOUD_FUNCTION_NAME \
  --project=$PROJECT_ID \
  --runtime=$RUNTIME \
  --trigger-http \
  --entry-point=$ENTRY_POINT \
  --region=$REGION
