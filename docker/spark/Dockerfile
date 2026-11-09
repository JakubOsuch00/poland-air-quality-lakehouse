FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY docker/jars.txt /tmp/jars.txt
RUN xargs -n1 curl -fsSL --retry 3 -O --output-dir ${SPARK_HOME}/jars < /tmp/jars.txt

ENV PYTHONPATH=/opt/aq/src \
    PIPELINE_CONFIG=/opt/aq/conf/pipeline.yaml
WORKDIR /opt/aq
COPY src ./src
COPY conf ./conf

ENTRYPOINT ["/usr/bin/tini", "--"]
