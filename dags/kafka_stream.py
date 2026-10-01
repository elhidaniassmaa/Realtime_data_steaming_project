import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 14, 12, 0),
    'retries': 1,
}

def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    return res.json()['results'][0]

def format_data(res):
    location = res['location']
    return {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'postcode': str(location['postcode']),
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium'],
    }

def stream_data():
    import json, time, logging
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    end_time = time.time() + 60
    sent = 0

    while time.time() < end_time:
        try:
            res = format_data(get_data())
            producer.send('user_created', json.dumps(res).encode('utf-8'))
            sent += 1
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error while streaming data: {e}")
            time.sleep(2)

    producer.flush()
    logging.info(f"{sent} messages envoyés")
    if sent == 0:
        raise RuntimeError("Aucun message envoyé : vérifier les logs")

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )