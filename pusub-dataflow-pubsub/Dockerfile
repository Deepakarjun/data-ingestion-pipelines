# Base on the latest stable Beam image
FROM apache/beam_python3.10_sdk:2.66.0

# Define working directory inside container (matches what Dataflow expects)
WORKDIR /template

# Copy pipeline file into /template/src/
COPY src/ps-df-ps.py ./src/ps-df-ps.py

# Entrypoint: Python pipeline
ENTRYPOINT ["python", "src/ps-df-ps.py"]
