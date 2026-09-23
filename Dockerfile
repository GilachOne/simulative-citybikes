FROM apache/airflow:2.11.0-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
RUN pip install --no-cache-dir 'apache-airflow==2.11.0' 'pyspark==3.5.6' 'psycopg2-binary==2.9.10' 'clickhouse-connect==0.8.17'
RUN mkdir -p /opt/airflow/state
ENV SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=/opt/airflow/project
WORKDIR /opt/airflow/project
