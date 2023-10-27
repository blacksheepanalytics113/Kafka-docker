import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import logging
from time import sleep

# default_args = {
#     'owner': 'airscholar',
#     'start_date': datetime(2023, 9, 3, 10, 00)
# }

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    # print(res)
    ress = res['results'][0]
    return ress
get_data()


def format_data():
    get_dataa = get_data()
    # print(get_dataa)
    data = {}
    location = get_dataa['location']
    # print(location)
    data['id'] = uuid.uuid4()
    # print(data['id'])
    get_dataa['first_name'] = get_dataa['name']['first']
    # print(get_dataa['first_name'])
    data['last_name'] = get_dataa['name']['last']
    data['gender'] = get_dataa['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = get_dataa['email']
    print(data['email'])
    data['username'] = get_dataa['login']['username']
    data['dob'] = get_dataa['dob']['date']
    data['registered_date'] = get_dataa['registered']['date']
    data['phone'] = get_dataa['phone']
    data['picture'] = get_dataa['picture']['medium']

    return data
    # print(data)
format_data()


# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging
# # 
#     producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
#     curr_time = time.time()

#     while True:
#         if time.time() > curr_time + 60: #1 minute
#             break
#         try:
#             res = get_data()
#             res = format_data(res)

#             producer.send('users_created', json.dumps(res).encode('utf-8'))
#         except Exception as e:
#             logging.error(f'An error occured: {e}')
#             continue





# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )