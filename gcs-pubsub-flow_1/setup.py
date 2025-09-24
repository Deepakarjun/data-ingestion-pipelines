# setup.py
import setuptools

setuptools.setup(
    name="gcs-streaming-to-pubsub",
    version="1.0.0",
    install_requires=[
        "apache-beam[gcp]>=2.67.0",
    ],
    packages=setuptools.find_packages() or ["."],
)
