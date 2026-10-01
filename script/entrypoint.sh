#!/bin/bash
set -e

# Échec explicite si les identifiants admin ne sont pas fournis (voir .env)
: "${AIRFLOW_ADMIN_USER:?Variable AIRFLOW_ADMIN_USER manquante (voir .env)}"
: "${AIRFLOW_ADMIN_PASSWORD:?Variable AIRFLOW_ADMIN_PASSWORD manquante (voir .env)}"

if [ -f /opt/airflow/requirements.txt ]; then
  pip install -r /opt/airflow/requirements.txt
fi

# Crée ou met à jour le schéma de la base de métadonnées
airflow db migrate

# Au redémarrage l'utilisateur existe déjà : ce n'est pas une erreur
airflow users create \
  --username "$AIRFLOW_ADMIN_USER" \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password "$AIRFLOW_ADMIN_PASSWORD" \
  || echo "Utilisateur admin déjà existant, création ignorée."

exec airflow "$@"