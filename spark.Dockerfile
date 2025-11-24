FROM bitnami/spark:3.5

USER root

# Install Python dependencies
COPY requirements.txt /app/
RUN --mount=type=cache,id=pip_cache_root,target=/root/.cache/pip \
    /opt/bitnami/python/bin/python3 -m pip install -r /app/requirements.txt

# Download Delta + Postgres jars
RUN mkdir -p /opt/bitnami/spark/jars \
    && curl -L -o /opt/bitnami/spark/jars/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl -L -o /opt/bitnami/spark/jars/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

RUN mkdir -p /app/spark-warehouse \
    && chmod -R 777 /app/spark-warehouse \
    && mkdir -p /app/data \
    && chmod -R 777 /app/data

USER 1001