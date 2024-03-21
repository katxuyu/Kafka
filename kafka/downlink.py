from flask import Flask, jsonify, request, Response
from datetime import datetime
import logging
import mysql.connector
from mysql.connector import errorcode
import sys


app = Flask(__name__)

import yaml

with open("./downlink.yml", 'r') as stream:
    try:
        config=yaml.safe_load(stream)
        print(config)
    except yaml.YAMLError as exc:
        print(exc)

log_path = config["api"]["downlink"]["log_path"]
X_API_KEY = config["api"]["downlink"]["key"]
db_host = config["api"]["downlink"]["database"]["host"]
db_user = config["api"]["downlink"]["database"]["user"]
db_password = config["api"]["downlink"]["database"]["password"]
db_name = config["api"]["downlink"]["database"]["name"]
db_table = config["api"]["downlink"]["database"]["table"]


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

dbconfig = {
    'host':db_host,
    'user':db_user,
    'password':db_password,
    'database':db_name #,
#    'client_flags': [mysql.connector.ClientFlag.SSL],
#    'ssl_ca': 'DigiCertGlobalRootG2.crt.pem'
}



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

if conn == "" or conn.is_connected()== False:      
    conn, cursor = db_connect()

def send_payload_to_db(sys_timestamp, app_id, payload):
    sql = "INSERT INTO %s (timestamp, application_id, raw_json) VALUES (%s, %s, %s)"

    if conn.is_connected() == False:
        conn.reconnect()
        global cursor
        cursor = conn.cursor()

    try:
        cursor.execute(sql, (db_table, sys_timestamp, app_id, payload,))
    except Exception as e:
        logger.error(f"ERROR: {e}")
    else:
        conn.commit()

    

@app.route("/api/v1/downlink", methods=['POST'])
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
        app_id = ""
        sys_timestamp = datetime.now()
        send_payload_to_db(sys_timestamp, app_id, payload)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    