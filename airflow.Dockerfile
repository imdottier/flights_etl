FROM apache/airflow:3.1.3

USER root

# Install system deps (apt-get can be cached too!)
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y default-jdk-headless procps

# Set JAVA_HOME so PySpark can find the JVM
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Install Python dependencies
COPY requirements.txt /app/
RUN --mount=type=cache,id=pip_cache_airflow,target=/home/airflow/.cache/pip \
    pip install -r /app/requirements.txt && \
    pip install apache-airflow-providers-fab
