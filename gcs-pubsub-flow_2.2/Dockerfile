FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

WORKDIR /template

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/template/setup.py"

COPY . /template/

RUN pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir -e . \
    && pip install --no-cache-dir -U "apache-beam[gcp]>=2.67.0"
