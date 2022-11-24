#!/usr/bin/env python3

import paho.mqtt.client as mqtt

import time
import json
import pyodbc
import datetime

server = 'kecollege.database.windows.net'
database = 'IoT_Analytics' 
username = 'CloudSA811fa360'  #CloudSA811fa360
password = 'Admin123$' 
driver= '{ODBC Driver 17 for SQL Server}'
status1=''
topic="control"
#Driver={ODBC Driver 13 for SQL Server};Server=tcp:kecollege.database.windows.net,1433;Database=IoT_Analytics;Uid=CloudSA811fa360;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
connection_string='DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
print(connection_string)
cnxn = pyodbc.connect(connection_string) 
cursor = cnxn.cursor()




# This is the Subscriber

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("control")

def on_message(client, userdata, msg):
  my_json=msg.payload.decode()
  res = json.loads(my_json)
  #process(res['data'],res['devaddr'])
 
  print("Message Recieved:", res)
  #client.disconnect()


def publish(client):
    msg_count = 0
    global status1
    sql_select_query = "select on_off_control from dbo.cloud_IOT"
        
    
    while True:
      # set variable in query
      cursor.execute(sql_select_query)
      # fetch result
      record = cursor.fetchall()
      machine_status=str(record[0][0])
      print(machine_status)
      machine_status=machine_status.strip()
         
      
      time.sleep(1)
       
      #machine_status for external off 
      if(machine_status=="off" or machine_status=="OFF"):
        msg = 'true'
      else:
        msg = 'false'
        
      result = client.publish(topic, msg)
        # result: [0, 1]
      status = result[0]
      if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
      else:
        print(f"Failed to send message to topic {topic}")
      msg_count += 1
  
client = mqtt.Client()
#client.username_pw_set("admin","admin")
client.connect("localhost",1883,60)

client.on_connect = on_connect
client.on_message = on_message
publish(client)
client.loop_forever()
