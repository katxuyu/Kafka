from flask import Flask, jsonify, request, Response
from datetime import datetime
import logging
from kafka import KafkaProducer
import configparser
import time
import socket
import json

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini')


log_path = config.get('rplogs', 'log_path')
https_auth = config.get('https', 'auth')
kafka_broker = config.get('kafka', 'broker')
kafka_topic = config.get('kafka', 'topic')

kafka_conf = {'bootstrap.servers': f"{kafka_broker}",
        'client.id': socket.gethostname()}

logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)



logger = logging.getLogger()
logger.setLevel(logging.INFO)

def receipt(err,msg):
    if err is not None:
        logger.error('ERROR: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)



def send_payload(t, m):
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(t, key=b'foo', value=m)



@app.route("/up-payloads", methods=['POST'])
def receive_payloads():
    
    auth = request.headers.get('Authorization')
    if not auth:
        return Response(
            "Authentication error: Authorization header is missing",
            status=401
        )
    parts = auth.split()

    if parts[0].lower() != "bearer":
        return Response("Authentication error: Authorization header must start with ' Bearer'", status=401)
    elif len(parts) == 1:
        return Response("Authentication error: Token not found", status=401)
    elif len(parts) > 2:
        return Response("Authentication error: Authorization header must be 'Bearer <token>'", status=401)
    
    elif auth != https_auth:
        return Response("Authentication error: Wrong Authorization", status=401)
    
    
    try:
        payload = request.get_json()
        send_payload("up-payloads",payload)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    
    

    
    return Response("Payload processed successfully.", status=200)


