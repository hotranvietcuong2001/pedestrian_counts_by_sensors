FROM apache/airflow:2.4.1

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq

COPY requirements.txt .

USER $AIRFLOW_UID

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME



