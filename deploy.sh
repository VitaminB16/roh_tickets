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
  --memory=1024MB

# Deploy the Cloud Scheduler jobs

# Job 1: Run events task daily at 13:00 GMT to update the events data for any new events and productions

# Job configuration
JOB_NAME="python-roh-update-events-daily"
LOCATION="europe-west2"
URI="https://europe-west2-vitaminb16.cloudfunctions.net/python-roh"
SCHEDULE="0 13 * * *"
TIME_ZONE="GMT"
PAYLOAD="{'task_name': 'events', 'no_plot': true}"
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
