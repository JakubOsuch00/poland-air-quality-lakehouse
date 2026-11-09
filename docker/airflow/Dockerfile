FROM apache/airflow:3.3.2-python3.11

USER root
# spark-submit runs the driver inside the airflow container -> needs a JVM
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark==6.3.4 -r /tmp/requirements.txt \
    # the provider pulls pyspark-client which overwrites files of the real pyspark package
    && pip uninstall -y pyspark-client \
    && pip install --no-cache-dir --no-deps --force-reinstall pyspark==4.0.1

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
COPY docker/jars.txt /tmp/jars.txt
RUN xargs -n1 curl -fsSL --retry 3 -O --output-dir ${SPARK_HOME}/jars < /tmp/jars.txt

ENV SPARK_CONF_DIR=/opt/spark-conf
COPY docker/spark/log4j2.properties ${SPARK_CONF_DIR}/log4j2.properties

ENV PYTHONPATH=/opt/aq/src \
    PIPELINE_CONFIG=/opt/aq/conf/pipeline.yaml
