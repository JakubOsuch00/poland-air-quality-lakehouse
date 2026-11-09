import os
import shutil

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


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
