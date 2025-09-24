# Dockerfile
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/template/setup.py"

COPY . /template/
WORKDIR /template

# RUN pip install --no-cache-dir -e .
RUN pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir -e . \
    && pip install --no-cache-dir -U "apache-beam[gcp]>=2.67.0"



ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
