#!/usr/bin/env python

import os
import json
import requests 
from datetime import datetime
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
import socket
import logging
from ast import literal_eval


import yaml

with open("./config.yml", 'r') as stream:
    try:
        config=yaml.safe_load(stream)
        #print(config)
    except yaml.YAMLError as exc:
        print(exc)


kafka_broker = config["general"]["kafka"]["broker"]
log_path = config["api"]["packetview"]["log_path"]
kafka_topic = config["api"]["packetview"]["kafka"]["topic"]
kafka_groupid = config["api"]["packetview"]["kafka"]["group.id"]
packetview_url= config["api"]["packetview"]["packetview_url"]


partition_number = sys.argv[1]

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
consumer.assign([TopicPartition(kafka_topic, partition_number)])

logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

max_batch_size = 20
partition_number = sys.argv[1]  

def process_data(data):
    try: 
        data = json.loads(data)
    except:
        data = literal_eval(data)
    #    print(data)
    try:
        data = json.loads(data)
        payload = data["payload"]
        key = data['key']
        ts = payload["uplink_message"]["received_at"]
#        print(key)

        final_url = packetview_url + key
        result = requests.post(final_url, json=payload)
        print(partition_number, ts, final_url, result.status_code)

    except Exception as e:
        print("Error encountered see errors.txt")
        with open("errors.txt", "a") as file:
            file.write(json.dumps(payload)+"\n")
            file.write(f"{e}\n")
        pass
        #exit(1)



MIN_COMMIT_COUNT = 1
running = True

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
                process_data(msg.value().decode("utf-8"))
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

consume_loop(consumer, [str(kafka_topic)])

