#!/usr/bin/env python

import os
import json
import requests as req
from datetime import datetime
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
import socket
import logging
from ast import literal_eval
import time
import csv

import yaml

with open("./config.yml", 'r') as stream:
    try:
        config=yaml.safe_load(stream)
        #print(config)
    except yaml.YAMLError as exc:
        print(exc)


kafka_broker = config["general"]["kafka"]["broker"]
log_path = config["api"]["lpr"]["log_path"]
kafka_topic = config["api"]["lpr"]["kafka"]["topic"]
kafka_groupid = config["api"]["lpr"]["kafka"]["group.id"]
target = config["api"]["lpr"]["target"]
data_path = config["api"]["lpr"]["data_path"]

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



dataset = []


def process_data(payload):
        global dataset
    #try:
        if len(dataset) == 50:
            with open(f"{data_path}/ltr-{time.strftime('%Y%m%d-%H:%M:%S')}.json", "a") as f:
                csv.writer(f, delimiter="\n").writerow(dataset)
            dataset = []
        else:
            dataset.append(payload)
        c_payload  = json.loads(str(payload).replace("'", '"'))
        for t in target:
            if (c_payload["category"]).lower() == t:
                p = c_payload["payload"]
                for url in target[t]:
                    res = req.post(url, json=p)
                    print(res, p)
                



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

