from flask import Flask, jsonify, request, Response
from datetime import datetime
import logging
from confluent_kafka import Producer
import configparser
import time
import socket
import json
import sys


app = Flask(__name__)

import yaml

with open("./config.yml", 'r') as stream:
    try:
        config=yaml.safe_load(stream)
        print(config)
    except yaml.YAMLError as exc:
        print(exc)

log_path = config["app"]["log_path"]
kafka_broker = config["general"]["kafka"]["broker"]
X_API_KEY = config["app"]["key"]
pwxpayloads_type = config["app"]["packetworx_types"]
pvw = config["app"]["packetview_types"]

kafka_conf = {'bootstrap.servers': f"{kafka_broker}",
        'client.id': socket.gethostname()}

producer = Producer(kafka_conf)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(sys.stdout)
    ]
)



logger = logging.getLogger()
logger.setLevel(logging.INFO)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))



def send_payload(t, m):
    print(kafka_broker)
    producer.produce(t, key="key", value=str(m).replace("'",'"').encode('utf-8'), callback=acked)



@app.route("/api/v1/up-noah", methods=['POST'])
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
    
    elif parts[1] != X_API_KEY:
        return Response("Authentication error: Wrong Authorization", status=401)
    
    
    try:
        payload = request.get_json()
        send_payload_to_kafka(pwxpayloads_type[3], "UP-NOAH", payload)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    
    

    
    return Response("Payload processed successfully.", status=200)

@app.route("/api/v1/license_data", methods=['POST'])
def receive_license_payloads():
    
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
    
    elif parts[1] != X_API_KEY:
        return Response("Authentication error: Wrong Authorization", status=401)
    # payload = request.get_json()
    # send_payload_to_eh(payload)   
    
    try:
        payload = request.get_json()
        send_payload_to_kafka(pwxpayloads_type[4], "LPR", payload)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    
    return Response("Payload processed successfully.", status=200)

def authorized(headers, app_id):
  print(headers)
  if ('x-api-key' in headers and headers['x-api-key'] == X_API_KEY) or ('v3authorization' in headers and headers['v3authorization'] == app_id):
    return True
  else:
    return False

def send_payload_to_kafka(client, category, payload):

    # payload = request.json
    if client in pwxpayloads_type:
        temp_payload = payload
        full_message = {"category": category, "payload": payload}
    else:
        temp_payload = payload['payload']
        full_message = payload

    if ("end_device_ids" in temp_payload and "application_ids" in temp_payload["end_device_ids"] and "uplink_message" in temp_payload) or category == 'INMARSAT':
        send_payload(client, full_message)
    else:
        print("Silently dropping invalid payload")


@app.route('/api/v1/ptdata', methods=['POST','GET'])
def receive_ptpayload():
  if request.method == 'GET':
    return "You performed a GET request. You need to do a POST request with a JSON payload"
  else:
    payload = request.json
    #print(payload)
    application_id = payload["end_device_ids"]["application_ids"]["application_id"]
    if authorized(request.headers, application_id):
      send_payload_to_kafka(pwxpayloads_type[0], 'PACKETTHINGS', payload)
      return 'OK', 200
    else:
      return 'Not Authorized', 401        


@app.route('/api/v1/payloads', methods=['POST','GET'])
def receive_payload():
  if request.method == 'GET':
    return "You performed a GET request. You need to do a POST request with a JSON payload"
  else:
    payload = request.json
    application_id = payload["end_device_ids"]["application_ids"]["application_id"]
    if authorized(request.headers, application_id):
      send_payload_to_kafka(pwxpayloads_type[1], 'TTI', payload)
      return 'OK', 200
    else:
      return 'Not Authorized', 401

@app.route('/api/v1/packetview/<key>', methods=['POST','GET'])
def receive_packetview(key):
  if request.method == 'GET':
    return "You performed a GET request with this key: " + key +".\n You need to do a POST request with a JSON payload"
  else:
    payload = {"key" : key, "payload" : request.json}
    send_payload_to_kafka(pvw[0], 'TTI', payload)
    return 'OK', 200
    #application_id = payload["end_device_ids"]["application_ids"]["application_id"]
    #if authorized(request.headers, application_id):
    #  send_payload_to_eh(packetview_client, 'TTI', payload)
    #  return 'OK', 200
    #else:
    #  return 'Not Authorized', 401      

@app.route('/api/v1/inmarsat', methods=['POST','GET'])
def receive_inmarsat():
  if request.method == 'GET':
    return "You performed a GET request. You need to do a POST request with a JSON payload"
  else:
    payload = request.json
    application_id = payload["application_id"]
    if authorized(request.headers, application_id):
      send_payload_to_kafka(pwxpayloads_type[2], 'INMARSAT', payload)
      return 'OK', 200
    else:
      return 'Not Authorized', 401

@app.route("/api/v1/alarm", methods=['POST'])
def receive_license_payloads():
    
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
    
    elif parts[1] != X_API_KEY:
        return Response("Authentication error: Wrong Authorization", status=401)
    # payload = request.get_json()
    # send_payload_to_eh(payload)   
    
    try:
        payload = request.get_json()
        send_payload_to_kafka(pwxpayloads_type[5], "ALARM", payload)
        
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    
    return Response("Payload processed successfully.", status=200)

if __name__ == "__main__":
    app.run()