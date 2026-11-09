import os
import shutil
from pathlib import Path

import pytest


def _java_available() -> bool:
    return bool(os.getenv("JAVA_HOME") or shutil.which("java"))


def pytest_collection_modifyitems(config, items):
    if _java_available():
        return
    skip = pytest.mark.skip(reason="no JVM available")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip)


def _delta_jars_bundled() -> bool:
    import pyspark

    jars = Path(pyspark.__file__).parent / "jars"
    return any(p.name.startswith("delta-spark") for p in jars.glob("*.jar"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    if not _delta_jars_bundled():
        # outside the docker image (e.g. CI) pull delta from maven
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
