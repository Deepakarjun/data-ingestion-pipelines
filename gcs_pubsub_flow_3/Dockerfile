FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

WORKDIR /template

# Copy wheel, main.py, setup.py, schemas
COPY dist/gcs_streaming_to_pubsub-1.0.0-py3-none-any.whl /template/
COPY main.py setup.py /template/
COPY schemas /template/schemas
COPY configs /template/configs
COPY costco /template/costco

# Install wheel and Apache Beam
RUN python3 -m pip install --no-cache-dir /template/gcs_streaming_to_pubsub-1.0.0-py3-none-any.whl \
    && python3 -m pip install --no-cache-dir -U "apache-beam[gcp]>=2.67.0"

# Flex template environment variables
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/template/setup.py"
