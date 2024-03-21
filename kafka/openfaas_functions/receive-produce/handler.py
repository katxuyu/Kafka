from flask import Flask, jsonify, request, Response
from datetime import datetime
import logging
from kafka import KafkaProducer
import configparser
import time
import socket
import json
import requests

app = Flask(__name__)

# config = configparser.ConfigParser()
# config.read('config.ini')

# https_auth = "20.24.19.71:9092"
kafka_broker = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "up-payloads"


# log_path = config.get('rplogs', 'log_path')
# https_auth = config.get('https', 'auth')
# kafka_broker = config.get('kafka', 'broker')
# kafka_topic = config.get('kafka', 'topic')

# kafka_conf = {'bootstrap.servers': f"{kafka_broker}",
#         'client.id': socket.gethostname()}

# logging.basicConfig(filename=log_path,
#                     filemode='a',
#                     format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                     datefmt='%H:%M:%S',
#                     level=logging.DEBUG)



logger = logging.getLogger()
logger.setLevel(logging.INFO)

def receipt(err,msg):
    if err is not None:
        logger.error('ERROR: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)


def send_payload(b, t, m):
    producer = KafkaProducer(bootstrap_servers='pkc-56d1g.eastus.azure.confluent.cloud:9092',
                             security_protocol="SASL_SSL",
                             sasl_mechanism="PLAIN",
                             sasl_plain_username="QYE5N4VSJU4XM2FH",
                             sasl_plain_password="cyfQL6HUiExwcPI+QyoeYGQGZZC+PDis8WaAYiWMvhuYHFj6a6kc57pnDGoqUwVJ",
                            value_serializer=lambda v: json.dumps(v).encode('utf-8')
                            )

    # producer = KafkaProducer(bootstrap_servers='20.24.19.71:9092',
    #                          security_protocol="SASL_SSL",
    #                          sasl_mechanism="PLAIN",
    #                          sasl_plain_username="admin",
    #                          sasl_plain_password="Qs9eqXV4x39b1HUitgsfuZSbLiY5MBOlmCiKbnRkddbQKzcVs8snh89uZEo3HWE",
    #                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
    #                         )
    producer.send(t, key=b'foo', value=m)







def handle(req):
    try:
        payload = json.loads(req)
        send_payload(kafka_broker, kafka_topic,payload)
    except Exception as e:
        logger.error(f"ERROR: {e}, {kafka_broker}")
        return Response(f"ERROR: {e}, {kafka_broker}", status=501)
    else:
        return Response("Payload processed successfully.", status=200)
