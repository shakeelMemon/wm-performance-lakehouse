"""Bronze layer: one Auto Loader streaming table per upstream feed.

Metadata-driven: the feed list lives in ``wm_common.config.FEEDS``; adding a new
source is a one-line config change, not a new pipeline.

Every bronze row carries lineage columns (_source_file, _file_modified_at,
_ingested_at, _source_system) and a ``_rescued_data`` column so schema drift never
drops data silently.
"""
import sys

import dlt
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("wm.src_root"))  # noqa: F821  (spark is provided by the runtime)
from wm_common.config import FEEDS  # noqa: E402

CATALOG = spark.conf.get("wm.catalog")          # noqa: F821
LANDING = spark.conf.get("wm.landing_path")     # noqa: F821


def _make_bronze(feed):
    reader_opts = {
        "cloudFiles.format": feed.fmt,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "rescuedDataColumn": "_rescued_data",
    }
    if feed.schema_hints:
        reader_opts["cloudFiles.schemaHints"] = feed.schema_hints
    if feed.fmt == "csv":
        reader_opts.update({"header": "true", "mode": "PERMISSIVE"})

    @dlt.table(
        name=f"{CATALOG}.wm_bronze.{feed.name}",
        comment=f"[bronze] {feed.description} — raw from {feed.source_system}",
        table_properties={
            "quality": "bronze",
            "wm.source_system": feed.source_system,
            "delta.enableChangeDataFeed": "true",
            "pipelines.reset.allowed": "false" if feed.name in ("transactions", "valuations") else "true",
        },
    )
    def bronze():
        return (
            spark.readStream.format("cloudFiles")   # noqa: F821
            .options(**reader_opts)
            .load(f"{LANDING}/{feed.name}")
            .withColumns({
                "_source_file": F.col("_metadata.file_path"),
                "_file_modified_at": F.col("_metadata.file_modification_time"),
                "_ingested_at": F.current_timestamp(),
                "_source_system": F.lit(feed.source_system),
            })
        )

    return bronze


for _feed in FEEDS:
    _make_bronze(_feed)
