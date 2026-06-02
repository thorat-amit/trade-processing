"""Package the pipeline modules + runtime deps for Dataflow workers.

Dataflow builds an sdist from this and pip-installs it on every worker when the
pipeline is launched with --setup_file=./setup.py.
"""
import setuptools

setuptools.setup(
    name="trade-etl-pipeline",
    version="1.0.0",
    description="Streaming trade ETL: Pub/Sub -> Beam validation -> Snowflake",
    packages=setuptools.find_packages(),
    py_modules=["pipeline", "snowflake_sink", "trade_schema"],
    install_requires=[
        "snowflake-connector-python[pandas]==3.12.3",
        "google-cloud-secret-manager==2.20.2",
        "pandas>=1.5,<2.2",
        "pyarrow>=10,<17",
        "cryptography>=41,<44",  # key-pair auth
    ],
)
