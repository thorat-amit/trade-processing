# Dataflow Flex Template image for the trade ETL streaming pipeline.
# Based on the official Beam Python SDK image matching the pipeline's Beam version.
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Copy pipeline source.
COPY pipeline.py snowflake_sink.py trade_schema.py setup.py requirements.txt ./
COPY transforms ./transforms

# Flex Template env: entrypoint + dependency files.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

# Install deps into the launcher image so template launch is fast.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
