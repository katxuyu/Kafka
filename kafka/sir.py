#!/usr/bin/env python

import os
import json
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
import requests as req

CONNECTION_STR = 'Endpoint=sb://pwxpayloads1.servicebus.windows.net/;SharedAccessKeyName=backuptofilesreader;SharedAccessKey=6m7y8dHwU3Y7lxjqtKGvKQSdxKno/2GDmfKbqckPmDU=;EntityPath=pwxpayloads'
EVENTHUB_NAME = 'pwxpayloads'
CONSUMER_GROUP = 'backuptofiles'
BLOB_STORAGE_CONTAINER = 'backuptofilescheckpointstore'
BLOB_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=pwxehcheckpointstore;AccountKey=8RXnBgPj0T7EbEpMwT2BEVRcYPAESSt4bdsbHQxGuxEmg1EOeGYPX/Umjbo/Nd7WvvR8yC94nc48+AStbVjWcQ==;EndpointSuffix=core.windows.net'

dbconfig = {
    'host':'packetthings-mysql-1.mysql.database.azure.com',
    'user':'packetthings_user',
    'password':'wRpv5lUu9dtjCtUGMR',
    'database':'packetthings' #,
#    'client_flags': [mysql.connector.ClientFlag.SSL],
#    'ssl_ca': 'DigiCertGlobalRootG2.crt.pem'
}

FILE_PATH="/gcpbackups/azurepayloads/"

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
    cursor = conn.cursor()


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


def send_to_noah(app_id, payload):
    HTTP_URL = "https://iot-noah.up.edu.ph/api/iot/"
    AUTH = "Bearer 81818df98fd7a07bc93ed7caaebbf996"
    header  = {'Authorization': AUTH}                                                                                         
    res = req.post(HTTP_URL, json=payload, headers=header, timeout=10)                                                         
    print(HTTP_URL, res.status_code)
#    res = req.post('https://eob7rvhme4p0ayz.m.pipedream.net', json=payload, headers=header, timeout=10)


def on_event(partition_context, event):
    # Put your code here.
    # If the operation is i/o intensive, multi-thread will have better performance.
    #print("Received event from partition: {}.".format(partition_context.partition_id))
    #print(event.body_as_str(encoding='UTF-8'))
    data = json.loads(event.body_as_str(encoding='UTF-8'))
    #print(data)
    try:
        payload = data["payload"]
        category = data["category"].upper()
        #print(category)
        if category in ["INMARSAT","TTI"]:
            payload_to_file(category, payload)

        elif category == "PACKETTHINGS":
            pt_to_db(payload)

        #{"end_device_ids": {"device_id": "th-0000000000006431", "application_ids": {"application_id": "winford-temphum"}
        #try:
        app_id = payload["end_device_ids"]["application_ids"]["application_id"]
        if app_id in ["noah-sublevel","noah-rain-gauge"]:
            send_to_noah(app_id, payload)
        #except:
        #    pass

    except Exception as e:
        print("Error encountered see errors.txt")
        with open("errors.txt", "a") as file:
            file.write(json.dumps(payload)+"\n")
            file.write(f"{e}\n")
        pass
        #exit(1)

    partition_context.update_checkpoint(event)


def on_partition_initialize(partition_context):
    # Put your code here.
    print("Partition: {} has been initialized.".format(partition_context.partition_id))


def on_partition_close(partition_context, reason):
    # Put your code here.
    print("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))


def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))


if __name__ == '__main__':
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_STORAGE_CONNECTION_STRING,
        BLOB_STORAGE_CONTAINER
    )
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store
    )

    try:
        with consumer_client:
            consumer_client.receive(
                on_event=on_event,
                on_partition_initialize=on_partition_initialize,
                on_partition_close=on_partition_close,
                on_error=on_error,
                starting_position="-1",  # "-1" is from the beginning of the partition.
            )
    except KeyboardInterrupt:
        print('Stopped receiving.')