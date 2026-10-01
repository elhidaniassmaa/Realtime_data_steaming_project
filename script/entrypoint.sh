#!/bin/bash
set -e

if [ -f /opt/airflow/requirements.txt ]; then
  pip install -r /opt/airflow/requirements.txt
fi

airflow db migrate

airflow users create \
  --username admin --firstname Admin --lastname Admin \
  --role Admin --email admin@example.com --password admin || true

exec airflow "$@"