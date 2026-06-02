"""Streaming trade ETL pipeline (Apache Beam / Dataflow).

Flow: Pub/Sub -> parse + static validation -> stateful version resolution ->
strip helper fields -> batch -> MERGE valid trades / append rejected trades into
Snowflake. Runs on the DirectRunner locally or on Dataflow in production.
"""
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
)
from apache_beam.transforms.util import BatchElements

from transforms.validation import (
    ParseAndStaticValidate,
    VersionResolve,
    StripInternalFields,
    TAG_VALID,
    TAG_REJECTED,
)
from snowflake_sink import (
    WriteValidTradesToSnowflake,
    WriteRejectedTradesToSnowflake,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("trade-pipeline")


class TradePipelineOptions(PipelineOptions):
    """Pipeline parameters, also surfaced as Flex Template parameters."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            required=True,
            help="Pub/Sub subscription: projects/<p>/subscriptions/<s>",
        )
        parser.add_argument(
            "--snowflake_secret",
            required=True,
            help="Secret Manager resource: projects/<p>/secrets/<name>/versions/latest",
        )
        parser.add_argument("--batch_max_size", type=int, default=500,
                            help="Max rows per Snowflake write batch.")
        parser.add_argument("--batch_max_seconds", type=float, default=15.0,
                            help="Max seconds to buffer before flushing a batch.")


def _batched_write(records, label, secret, writer, opts):
    """Batch records on size/time, then write them to Snowflake.

    max_batch_duration_secs matters in streaming: bundles are usually tiny, so
    without a time bound the batches would never fill.
    """
    return (
        records
        | f"Batch{label}" >> BatchElements(
            min_batch_size=1,
            max_batch_size=opts.batch_max_size,
            max_batch_duration_secs=opts.batch_max_seconds,
        )
        | f"Write{label}" >> beam.ParDo(writer(secret))
    )


def build_pipeline(p, opts):
    secret = opts.snowflake_secret

    raw = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=opts.input_subscription)

    # Stage 1: parse + the stateless checks (malformed, past maturity).
    parsed = raw | "ParseAndStaticValidate" >> beam.ParDo(
        ParseAndStaticValidate()
    ).with_outputs(TAG_REJECTED, TAG_VALID)

    # Stage 2: version resolution (reject lower, replace equal, accept higher).
    versioned = parsed[TAG_VALID] | "VersionResolve" >> beam.ParDo(
        VersionResolve()
    ).with_outputs(TAG_REJECTED, TAG_VALID)

    valid_records = versioned[TAG_VALID] | "StripInternal" >> beam.ParDo(StripInternalFields())

    # Rejections come from both stages; merge them into one stream for the sink.
    rejected_records = (
        (parsed[TAG_REJECTED], versioned[TAG_REJECTED]) | "FlattenRejected" >> beam.Flatten()
    )

    _batched_write(valid_records, "Valid", secret, WriteValidTradesToSnowflake, opts)
    _batched_write(rejected_records, "Rejected", secret, WriteRejectedTradesToSnowflake, opts)


def run(argv=None):
    opts = TradePipelineOptions(argv)
    opts.view_as(StandardOptions).streaming = True
    # Modules are shipped to workers via --setup_file, so we don't pickle __main__.
    opts.view_as(SetupOptions).save_main_session = False

    with beam.Pipeline(options=opts) as p:
        build_pipeline(p, opts.view_as(TradePipelineOptions))
    log.info("pipeline submitted")


if __name__ == "__main__":
    run()
