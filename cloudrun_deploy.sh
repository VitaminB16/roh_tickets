#!/bin/bash

cp .gcloudignore .dockerignore

poetry export -f requirements.txt --output requirements.txt --without-hashes

# Convert .env file to .env.yaml by removing GOOGLE_APPLICATION_CREDENTIALS and replacing '=' with ':', adding double quotes around values
awk -F= '{print $1 ": \"" $2 "\""}' .env | grep -v GOOGLE_APPLICATION_CREDENTIALS >yamls/cloudrun.env.yaml

echo "SERVE_AS: \"cloud_run\"" >>yamls/cloudrun.env.yaml

PROJECT_ID="vitaminb16"
CLOUD_FUNCTION_NAME="python-roh"
REGION="europe-west2"

docker build -t gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest .

docker push gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest

gcloud run deploy ${CLOUD_FUNCTION_NAME} \
  --image gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest \
  --platform managed \
  --region ${REGION} \
  --project=${PROJECT_ID} \
  --env-vars-file yamls/cloudrun.env.yaml
