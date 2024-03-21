from confluent_kafka import Consumer, KafkaError, KafkaException
import configparser
import logging
import json
from ast import literal_eval
import numpy
import pandas
import psycopg2
from datetime import datetime
import os

import csv

import yaml



with open("./config.yml", 'r') as stream:
    try:
        config=yaml.safe_load(stream)
        #print(config)
    except yaml.YAMLError as exc:
        print(exc)

log_path = config["api"]["alarm"]["log_path"]
data_folder1 = config["api"]["alarm"]["data_folder1"]
data_folder2 = config["api"]["alarm"]["data_folder2"]
kafka_broker = config["general"]["kafka"]["broker"]
kafka_topic = config["api"]["alarm"]["kafka"]["topic"]
kafka_groupid = config["api"]["alarm"]["kafka"]["group.id"]
db_host = config["api"]["alarm"]["database"]["host"]
db_user = config["api"]["alarm"]["database"]["user"]
db_password = config["api"]["alarm"]["database"]["password"]
db_name = config["api"]["alarm"]["database"]["name"]
db_table = config["api"]["alarm"]["database"]["table"]
db_port = config["api"]["alarm"]["database"]["port"]


logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(pathname)s:%(lineno)d %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

dbconfig = {
    'host':db_host,
    'user':db_user,
    'password':db_password,
    'database':db_name,
    'port': int(db_port)
#    'client_flags': [mysql.connector.ClientFlag.SSL],
#    'ssl_ca': 'DigiCertGlobalRootG2.crt.pem'
}


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
conn = ""
cur = ""

def db_connect():
    try:
        conn = psycopg2.connect(**dbconfig)
    except Exception as err:
        print(err)
        exit(1)
    else:
        return conn, conn.cursor()

def alarm_exists(alarm_id):
    sql = f"SELECT * FROM {db_table} WHERE alarm_id = '{alarm_id}'"
    cur = conn.cursor()
    ret = None
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(f"{alarm_id} - {e}")
    else:
        conn.commit()
    finally:
        ret = cur.fetchone()
        cur.close()

    return ret

def alarm_to_db(alarm_id):
    sql = f"INSERT INTO {db_table} (alarm_id) VALUES ('{alarm_id}')"
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(f"{alarm_id} - {e}")
    else:
        conn.commit()
    finally:
        cur.close()

fields = ['UniqueID', 'Alarm Name', 'Alarm ID', 'Occurence Time', 'Severity', 'Device Name', 'Alarm Type', 'Additional Info', 'NE Type', 'Status']
for folder in (data_folder1+"/tosend", data_folder2+"/tosend"):
    if not os.path.exists(folder):
        os.makedirs(folder)

def save_to_csv(data):
    print(data)
    filename2 = f'{data_folder1}/AlarmList.csv'
    filename1 = f'{data_folder2}/AlarmList.csv'
    file_exists1 = os.path.isfile(filename1)
    file_exists2 = os.path.isfile(filename2)

    try:
        with open(filename1, 'a') as f:
            write = csv.writer(f)
            writer = csv.DictWriter(f, delimiter=',', lineterminator='\n',fieldnames=fields)

            if not file_exists1:
                writer.writeheader()  # file doesn't exist yet, write a header
            write.writerows(data)
            
    except Exception as e:
        logging.error(e)
    
    try:
        with open(filename2, 'a') as f:
            write = csv.writer(f)
            writer = csv.DictWriter(f, delimiter=',', lineterminator='\n',fieldnames=fields)

            if not file_exists2:
                writer.writeheader()  # file doesn't exist yet, write a header
            write.writerows(data)
            
    except Exception as e:
        logging.error(e)

def payload_to_file(category, payload):
    global conn, cursor
    if conn == "" or conn.is_connected() == False:
        conn, cur = db_connect()

    if category == "ALARM":
        id = payload["id"]["id"]
        alarm = alarm_exists(id)
        while alarm == None:
            alarm_to_db(id)
            alarm = alarm_exists(id)
        
        
        UniqueId = alarm[0]
            
        alarm_id = payload["name"]
        alarm_type = payload["name"]
        alarm_name = f"{payload['name']} Alert"
        severity = payload["severity"]
        cleared = payload["cleared"]

        if cleared:
            status = "UP"
            occurence_time = datetime.utcfromtimestamp(int(payload["clearTs"])/1000).strftime('%m/%d/%Y %H:%M:%S')
        else:
            status = "DOWN"
            occurence_time = datetime.utcfromtimestamp(int(payload["startTs"])/1000).strftime('%m/%d/%Y %H:%M:%S')

        additional_info = payload["details"]["data"][1:-1] if "data" in payload["details"] else ""
        device_name = payload["originatorName"]
        ne_type = payload["deviceType"]

        data = [[UniqueId, alarm_name, alarm_id, occurence_time, severity, device_name, alarm_type, additional_info, ne_type, status]]
        save_to_csv(data)

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
  
    except Exception as e:
        logging.error(e)
    
        
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
###msg_process(pp)
