import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging
# from cassandra.cluster import Cluster
import pymongo
from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType
from Get_Data_dag import  get_data
from Get_Data_dag import format_data
from kafka import KafkaProducer
import logging
from time import sleep
import json
from pykafka import KafkaClient



# def create_connection():
#     print('Connecting to the Mongo database...')
#     # try:
#     moongo_uri = "mongodb://localhost:27017"
#     database_name = "kafka_admin"
#     collection_name = "api_data"
    
#     client = pymongo.MongoClient(moongo_uri)
    
#     # Accesss database if exists
#     data_created = client[database_name]
    
#     # create a colllection in database 
#     collection_created = data_created[collection_name]
    
#     json_object = format_data()
#     print(json_object)
    # # Insert Bulk Data 
    # result = json_object.insert_one()
    # result.inserted_ids
    


    # except (Exception, pymongo.DatabaseError) as error:
    #     print(error)
    # finally:
    #     if client is not None:
    #         client.close()
#     print('Database connection closed.')

# create_connection()
def stream_data():
    Kafka_host = "137.184.109.96:9000"
    client = KafkaClient(hosts=Kafka_host)
    topic = client.topics["Stream_Api_Dataa"]
    
    with topic.get_sync_producer() as producer:
        for i in range(10):
            message = get_data()
            message_1 = format_data()
            encoded_message = message.encode("utf-8")
            producer.produce(encoded_message)
stream_data()
    
    # def get_partition(key,all,available): # Sending the data to only one partition in the broker 
    #     return 0
    # producer = KafkaProducer(bootstrap_servers=['137.184.109.96:9000'],partitioner = get_partition,
    #                          broker = '1001'
    #                         acks='all',
    #                         retries = 'restart')
    # print(f'Initialize Kafka Producer at {datetime.now()}')
    # # Set a basic message counter and dereive data from transformed data
    # counter = 0
    
    # res = get_data()
    # res = format_data()
    # def get_partition(key,all,available): # Sending the data to only one partition in the broker 
    #     return 0
    # # Set the counter as the message 
    # key = str(counter).encode()
    
    # Data = json.dumps(res,default= str).encode('utf-8')
    # # Send Data To Kafka Topic 
    # producer.send(topic="Stream_Api_Dataa",key=key,value=Data)
    
    # sleep(0.2)
    # counter = counter +1
    # print(f'Sent record at {datetime.now()}')
stream_data()
