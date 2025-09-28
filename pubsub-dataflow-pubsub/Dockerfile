FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

# Define working directory inside container (matches what Dataflow expects)
WORKDIR /template

# Copy pipeline file into /template/src/
COPY src/ps-df-ps.py ./src/ps-df-ps.py
COPY configs ./configs

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/src/ps-df-ps.py"

# Entrypoint: Python pipeline
# ENTRYPOINT ["python", "src/ps-df-ps.py"]
