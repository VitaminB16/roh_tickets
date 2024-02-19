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
  --env-vars-file yamls/cloudrun.env.yaml \
  --memory=1Gi

####################
# Cloud Scheduler  #
####################

# Job 1: Run events task daily at 13:00 GMT to update the events data for any new events and productions

# Job configuration
JOB_NAME="python-roh-update-events-daily"
LOCATION="europe-west2"
URI="https://python-roh-jfzraqzsma-nw.a.run.app"
SCHEDULE="0 9,12,15,18 * * *"
TIME_ZONE="GMT"
PAYLOAD='{"task_name": "events"}'
OIDC_SERVICE_ACCOUNT_EMAIL="vitaminb16@vitaminb16.iam.gserviceaccount.com"

configure_job() {
  gcloud scheduler jobs $1 http $JOB_NAME \
    --location $LOCATION \
    --schedule "$SCHEDULE" \
    --time-zone "$TIME_ZONE" \
    --uri "$URI" \
    --http-method POST \
    --message-body "$PAYLOAD" \
    --oidc-service-account-email $OIDC_SERVICE_ACCOUNT_EMAIL
}

# If the job exists, use update, otherwise use create
if gcloud scheduler jobs describe $JOB_NAME --location $LOCATION &>/dev/null; then
  ACTION="update"
else
  ACTION="create"
fi

echo "${ACTION} the job..."
configure_job $ACTION