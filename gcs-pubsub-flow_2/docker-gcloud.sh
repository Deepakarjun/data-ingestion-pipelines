export PROJECT_ID=terraform-test-gcp-0114
echo $PROJECT_ID
export IMAGE_NAME="gcs-stream-flow"
echo $IMAGE_NAME
export REPO_NAME="dataflow-flex-repo"
echo $REPO_NAME
export BUCKET_NAME="test-configurations-0114"
echo $BUCKET_NAME
export REGION="us-east1"
echo $REGION
export TAG="202009162"
echo $TAG
export NAME_FLEX_TEMPLATE="gcs-stream-flex-template.json"
echo $NAME_FLEX_TEMPLATE

# Create Artifact Registry repository (if it doesn't exist)
# gcloud artifacts repositories create $REPO_NAME --repository-format=docker --location=$REGION

# Submit the build

gcloud config set project terraform-test-gcp-0114

gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME:$TAG .

gcloud dataflow flex-template build gs://$BUCKET_NAME/templates/$NAME_FLEX_TEMPLATE \
  --image=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME:$TAG \
  --sdk-language=PYTHON \
  --metadata-file=metadata.json

gcloud dataflow flex-template run "gcs-stream-$(date +%Y%m%d%H%M%S)" \
  --template-file-gcs-location="gs://$BUCKET_NAME/templates/$NAME_FLEX_TEMPLATE" \
  --region="$REGION" \
  --parameters input_bucket=gs://$BUCKET_NAME/data/*.json \
  --parameters output_topic=projects/$PROJECT_ID/topics/eggress_costco_topic \
  --parameters interval_sec=30 \
  --parameters expected_date_yyyymmdd=$(date +%Y%m%d) \
  --parameters schema_path=gs://$BUCKET_NAME/schemas/default_schema.json