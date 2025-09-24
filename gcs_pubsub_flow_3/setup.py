# setup.py
import setuptools

setuptools.setup(
    name="gcs_streaming_to_pubsub",
    version="1.0.0",
    install_requires=[
        "apache-beam[gcp]>=2.67.0",
    ],
    packages=setuptools.find_packages() + ["schemas"] or ["."],
    package_data={
        "schemas": ["*.json"],
    },
    include_package_data=True
)
