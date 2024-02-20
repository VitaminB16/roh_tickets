#!/bin/bash

PROJECT_ID="vitaminb16"
CLOUD_FUNCTION_NAME="python-roh-dash"
REGION="europe-west2"
YAML_FILE="yamls/dash.env.yaml"
DOCKERFILE="DockerfileDash"
MEMORY="1Gi"

# The Dockerfile for the Dash app is the same as the one for the Cloud Run app, except for the SERVE_AS environment variable
cp Dockerfile ${DOCKERFILE}
# Use an if statement to check for sed version and apply the appropriate command
if sed --version 2>/dev/null | grep -q GNU; then
  # GNU sed
  sed -i 's/ENV SERVE_AS="cloud_run"/ENV SERVE_AS="dash_app"/' ${DOCKERFILE}
else
  # BSD sed
  sed -i '' 's/ENV SERVE_AS="cloud_run"/ENV SERVE_AS="dash_app"/' ${DOCKERFILE}
fi
cp .gcloudignore .dockerignore

poetry export -f requirements.txt --output requirements.txt --without-hashes

# Convert .env file to .env.yaml by removing GOOGLE_APPLICATION_CREDENTIALS and replacing '=' with ':', adding double quotes around values
awk -F= '{print $1 ": \"" $2 "\""}' .env | grep -v GOOGLE_APPLICATION_CREDENTIALS >${YAML_FILE}

echo "SERVE_AS: \"cloud_run\"" >>${YAML_FILE}

docker build -t gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest -f ${DOCKERFILE} .

docker push gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest

gcloud run deploy ${CLOUD_FUNCTION_NAME} \
  --image gcr.io/${PROJECT_ID}/${CLOUD_FUNCTION_NAME}:latest \
  --platform managed \
  --region ${REGION} \
  --project=${PROJECT_ID} \
  --env-vars-file ${YAML_FILE} \
  --memory=${MEMORY}
