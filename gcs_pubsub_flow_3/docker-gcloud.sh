# Export Variables
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
export TAG="t9"
echo $TAG
export NAME_FLEX_TEMPLATE="gcs-stream-flex-template.json"
echo $NAME_FLEX_TEMPLATE


# Remove previous Built packages
rm -rf dist
rm -rf build
rm -rf *.egg-info

# Build packages with the while
# python3 -m build --wheel
python3 setup.py sdist bdist_wheel

# set project ID
gcloud config set project terraform-test-gcp-0114

# Build image and push
gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME:$TAG .

# Build flex template
gcloud dataflow flex-template build gs://$BUCKET_NAME/templates/$NAME_FLEX_TEMPLATE \
  --image=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME:$TAG \
  --sdk-language=PYTHON \
  --metadata-file=metadata.json

# Run flex template
gcloud dataflow flex-template run "gcs-stream-$(date +%Y%m%d%H%M%S)" \
  --template-file-gcs-location="gs://$BUCKET_NAME/templates/$NAME_FLEX_TEMPLATE" \
  --region="$REGION" \
  --parameters pipeline_options=/template/configs/pipeline_options.json \
  --parameters io_options=/template/configs/io_options.json