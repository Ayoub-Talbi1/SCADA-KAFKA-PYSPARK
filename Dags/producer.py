
import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'MMA',
    'start_date': datetime(2024, 1, 14, 14, 00)
}



def get_data():
    import requests
    URL = 'http://scada:8000'
    response = requests.get(URL)
    data = response.json()
    print(data)
    return data

def send2kafka():
    import time
    from kafka import KafkaProducer 
    import json
    curr = time.time()
    data = get_data()
    producer = KafkaProducer(bootstrap_servers=["kafka1:29092"], max_block_ms=5000)
    producer.send("testTopic", json.dumps(data).encode('utf-8'))



with DAG('stream-data',
         default_args=default_args,
         schedule_interval=timedelta(seconds=2),
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=send2kafka,
        dag=dag
    )

    consuming_task = BashOperator(
        task_id='consume_data',
        bash_command='spark-submit --master spark://spark-master:8080 consumer.py',
        dag=dag
    )

streaming_task >> consuming_task