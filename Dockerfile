# One image for every Python workload in the project:
#   * the pipeline CLI (`docker compose run --rm app python -m src.cli ...`)
#   * PySpark jobs (local[*] or the optional standalone cluster)
#   * Apache Airflow components (api-server, scheduler, dag-processor)
# Using a single image guarantees the driver, executors and Airflow workers share
# the same Python (3.12), Java (17), Spark (4.1.1) and connector JAR versions.
FROM apache/airflow:3.1.8-python3.12

ARG AIRFLOW_VERSION=3.1.8
ARG PYTHON_VERSION=3.12
# Spark 4.1.1 is built against Hadoop 3.4.2 -> matching S3A connector + AWS SDK v2 bundle.
ARG HADOOP_AWS_VERSION=3.4.2
ARG AWS_SDK_V2_VERSION=2.29.52
ARG ANALYTICS_ACCELERATOR_VERSION=1.2.1
ARG POSTGRES_JDBC_VERSION=42.7.14

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    SPARK_JARS_DIR=/opt/spark-jars

RUN mkdir -p ${SPARK_JARS_DIR} && cd ${SPARK_JARS_DIR} \
    && MAVEN=https://repo1.maven.org/maven2 \
    && curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSLO ${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar \
    && curl -fsSLO ${MAVEN}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${ANALYTICS_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${ANALYTICS_ACCELERATOR_VERSION}.jar \
    && curl -fsSLO ${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    && chmod 644 *.jar

USER airflow
COPY --chown=airflow:root requirements.txt requirements-dev.txt /tmp/
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements-dev.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Expose the pip-installed Spark distribution as SPARK_HOME so `spark-class`
# (used by the optional standalone cluster) and spark-submit are available.
USER root
RUN ln -s "$(su airflow -c "python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))'")" /opt/spark
USER airflow
ENV SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/project \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python

WORKDIR /opt/project
