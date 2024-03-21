from confluent_kafka import Consumer, KafkaError, KafkaException
import configparser
import socket
import logging
import requests as req
import json
import sys
from ast import literal_eval
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode

import yaml

with open("./config.yml", 'r') as stream:
    try:
        config=yaml.safe_load(stream)
        print(config)
    except yaml.YAMLError as exc:
        print(exc)


log_path = config["api"]["payload_backup"]["log_path"]
kafka_broker = config["general"]["kafka"]["broker"]
kafka_topic = config["api"]["payload_backup"]["kafka"]["topic"]
kafka_groupid = config["api"]["payload_backup"]["kafka"]["group.id"]
FILE_PATH=config["api"]["payload_backup"]["data_path"]

logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))

conf = {'bootstrap.servers': f"{kafka_broker}",
        'group.id': f"{kafka_groupid}",
        'enable.auto.commit': False,
        'default.topic.config': {'auto.offset.reset': 'earliest'},
        'on_commit': commit_completed}

consumer = Consumer(conf)
running = True

def payload_to_file(category, payload):
    if category == "INMARSAT":
        #print(category)
        app_id = payload["application_id"]
        temp_time_str = payload["mailboxTimeUtc"]
        date_today=temp_time_str[0:10]
    else:  #"TTI"
        app_id = payload["end_device_ids"]["application_ids"]["application_id"]
        temp_time_str = payload["uplink_message"]["received_at"]
                                                            
    date_today=temp_time_str[0:10]
    file_name=f"{FILE_PATH}{app_id}-{date_today}.json"
    print(f"{temp_time_str}: {file_name}")
    with open(file_name, "a") as file:
        file.write(json.dumps(payload)+"\n")



def msg_process(msg):
    try: 
        data = json.loads(msg)
    except:
        data = literal_eval(msg)

    try:
        payload = data["payload"]
        category = data["category"].upper()
        #print(category)
        payload_to_file(category, payload)

       
    except:
        pass
    
        
MIN_COMMIT_COUNT = 1   

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg.value().decode("utf-8"))
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

consume_loop(consumer, [str(kafka_topic)])

