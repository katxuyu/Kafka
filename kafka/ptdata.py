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

config = configparser.ConfigParser()
config.read('config.ini')



import yaml

with open("./config.yml", 'r') as stream:
    try:
        config=yaml.safe_load(stream)
        print(config)
    except yaml.YAMLError as exc:
        print(exc)

log_path = config["api"]["ptdata"]["log_path"]
kafka_broker = config["general"]["kafka"]["broker"]
kafka_topic = config["api"]["ptdata"]["kafka"]["topic"]
kafka_groupid = config["api"]["ptdata"]["kafka"]["group.id"]
db_host = config["api"]["ptdata"]["database"]["host"]
db_user = config["api"]["ptdata"]["database"]["user"]
db_password = config["api"]["ptdata"]["database"]["password"]
db_name = config["api"]["ptdata"]["database"]["name"]

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

dbconfig = {
    'host':db_host,
    'user':db_user,
    'password':db_password,
    'database':db_name #,
#    'client_flags': [mysql.connector.ClientFlag.SSL],
#    'ssl_ca': 'DigiCertGlobalRootG2.crt.pem'
}


payload_dict = {
  'altitude' : 'altitude',
  'active_energy_net_1' : 'active_energy_net',
  'barometric' : 'barometric',
  'batt_v' : 'battery',
  'battery' : 'battery',
  'batt_percent' : 'battery',
  'co2' : 'co2',
  'eco2' : 'co2',
  'current' : 'current',
  'currentRms_amps' : 'current',
  'float_current' : 'current',
  'current_l1' : 'current_l1',
  'current_l2' : 'current_l2',
  'current_l3' : 'current_l3',
  'daily_volume' : 'daily_volume',
  'distance' : 'distance',
  'distance_m' : 'distance_m',
  'energy' : 'energy',
  'float_energy' : 'energy',
  'hcho' : 'hcho',
  'hum_rh' : 'humidity',
  'humidity' : 'humidity',
  'rh' : 'humidity',
  'in' : 'in',
  'installed' : 'installed',
  'instantaneous_flow' : 'instantaneous_flow',
  'light_level' : 'light_level',
  'out' : 'out',
  'float_power' : 'power',
  'power' : 'power',
  'pm_0_1' : 'pm1',
  'pm_1_0' : 'pm1',
  'pm1' : 'pm1',
  'pm1_0' : 'pm1',
  'pm_10_0' : 'pm10',
  'pm10' : 'pm10',
  'pm_2_5' : 'pm2_5',
  'pm2_5' : 'pm2_5',
  'pressure' : 'pressure',
  'reverse_flow' : 'reverse_flow',
  'fire_alarm' : 'state',
  'leak_status' : 'state',
  'switch_state' : 'state',
  'state' : 'state',
  'amb_temp' : 'temperature',
  'temp' : 'temperature',
  'temperature' : 'temperature',
  'contact_temp' : 'temperature',
  'temp_c' : 'temperature',
  'total_active_energy' : 'total_active_energy',
  'tvoc' : 'tvoc',
  'float_voltage' : 'voltage',
  'voltage' : 'voltage',
  'voltage_l1' : 'voltage_l1',
  'voltage_l2' : 'voltage_l2',
  'voltage_l3' : 'voltage_l3',
  'volume' : 'volume'
}



consumer = Consumer(conf)

running = True
conn = ""
cursor = ""
def db_connect():
    try:
        conn = mysql.connector.connect(**dbconfig)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        exit(1)
    else:
        return conn, conn.cursor()



def pt_to_db(payload):
    sql = "INSERT INTO device_data (ts, dev_eui, measurement, value, source_application_id) VALUES (%s, %s, %s, %s, %s)"
    dev_eui = payload["end_device_ids"]["dev_eui"]
    application_id = payload["end_device_ids"]["application_ids"]["application_id"]
    ts = datetime.strptime(payload["uplink_message"]["received_at"][0:26], "%Y-%m-%dT%H:%M:%S.%f")

    if "decoded_payload" not in payload["uplink_message"]:
        return

    if conn.is_connected() == False:
        conn.reconnect()
        global cursor
        cursor = conn.cursor()

    for key, value in payload["uplink_message"]["decoded_payload"].items():
        if isinstance(value, (int, float)) :  
            try:
                key = payload_dict[key.lower()]
            except:
                #key = "U+" + key
                continue
            print(f"Insert to DB: {ts}, {dev_eui}, {key}, {value}, {application_id}")
            try:
                cursor.execute(sql, (ts, dev_eui, key, value, application_id))
            except:
                pass
    conn.commit()

def msg_process(msg):
    try: 
        data = json.loads(msg)
    except:
        data = literal_eval(msg)

    try:
        payload = data["payload"]
        global conn, cursor
        if conn == "" or conn.is_connected()== False:      
            conn, cursor = db_connect()
            pt_to_db(payload)
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

