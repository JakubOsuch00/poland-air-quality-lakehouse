import importlib.util

if importlib.util.find_spec("pyspark") is None:
    collect_ignore_glob = ["test_*.py"]
