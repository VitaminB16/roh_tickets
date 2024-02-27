#!/bin/bash

PROJECT_ID="vitaminb16"
CLOUD_FUNCTION_NAME="python-roh-dash"
REGION="europe-west2"
YAML_FILE="yamls/dash.env.yaml"
MEMORY="1Gi"
SERVE_AS="dash_app"

# The Dockerfile for the Dash app is the same as the one for the Cloud Run app
cp .gcloudignore .dockerignore

poetry export -f requirements.txt --output requirements.txt --without-hashes

# Convert .env file to .env.yaml by removing GOOGLE_APPLICATION_CREDENTIALS and replacing '=' with ':', adding double quotes around values
awk -F= '{print $1 ": \"" $2 "\""}' .env | grep -v GOOGLE_APPLICATION_CREDENTIALS >${YAML_FILE}

echo "SERVE_AS: \"${SERVE_AS}\"" >>${YAML_FILE}

docker build . -t gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest

docker push gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest

gcloud run deploy ${CLOUD_FUNCTION_NAME} \
  --image gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest \
  --platform managed \
  --region ${REGION} \
  --project=${PROJECT_ID} \
  --env-vars-file ${YAML_FILE} \
  --memory=${MEMORY} \
  --min-instances=1
