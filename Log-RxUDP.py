#!/usr/bin/env python

# Rx UDP json messages and insert into database

import socket
import json
import os
import MySQLdb
import datetime

configfilename = "Log-config.json"

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return True

# open log_config.json and read
configfile = open(configfilename, 'r')
configfilejson = configfile.read()
configfile.close()

# parse configfilejson
json_data = json.loads(configfilejson)

LOG_IP = json_data['LOG_IP']
LOG_PORT = int(json_data['LOG_PORT'])
LOG_BUFFER_SIZE = int(json_data['LOG_BUFFER_SIZE'])

RFID_FIX_TX_IP = json_data['RFID_FIX_TX_IP']
RFID_FIX_TX_PORT = int(json_data['RFID_FIX_TX_PORT'])

RFID_FIX_RX_IP = json_data['RFID_FIX_RX_IP']
RFID_FIX_RX_PORT = int(json_data['RFID_FIX_RX_PORT'])

SQL_Host = json_data['SQL_Host']
SQL_User = json_data['SQL_User']
SQL_Passwd = json_data['SQL_Passwd']
SQL_DB = json_data['SQL_DB']

LOG_ADDR = (LOG_IP, LOG_PORT)
RFID_FIX_TX_ADDR = (RFID_FIX_TX_IP, RFID_FIX_TX_PORT)
RFID_FIX_RX_ADDR = (RFID_FIX_RX_IP, RFID_FIX_RX_PORT)

# Open database connection
db = MySQLdb.connect(host=SQL_Host,     # your host, usually localhost
                     user=SQL_User,     # your username
                     passwd=SQL_Passwd, # your password
                     db=SQL_DB)         # name of the data base

cursor = db.cursor()  # prepare a cursor object using cursor() method

# Initialise UDP Log server
sLOG = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sLOG.bind(LOG_ADDR)

# process received packets
while True:
   Rx_log_msg, addr = sLOG.recvfrom(LOG_BUFFER_SIZE)
   Client_IP = addr[0]
   Client_PORT = addr[1]

   if is_json(Rx_log_msg):
      json_data = json.loads(Rx_log_msg)
      msg_type = json_data['Msg_Type']
      if msg_type = "GPS":
#        Process_GPS_msg(json_data)          # to put all below in one line
         try: # read the json info
            Car_Name = json_data['Car_Name']
            Latitude = json_data['tpv'][0]['lat']
            Longitude = json_data['tpv'][0]['lon']
            Speed = json_data['tpv'][0]['speed']
            GMTdatetime = datetime.datetime.strptime(json_data['tpv'][0]['time'], '%Y-%m-%dT%H:%M:%S')
            CSTdatetime = GMTdatetime + datetime.timedelta(hours=9,mins=30)

         except: # just get the next message
            continue
         try: # insert info into database
            sql_query = "INSERT INTO GPS_log(Datetime, Car, Longitude, Latitude, Speed) \
                         VALUES ('%s', '%s', '%s', '%s', '%s')" \
                         % (CSTdatetime, Car_Name, Longitude, Latitude, Speed)
            cursor.execute(sql_query)
            db.commit()
         except:
            db.rollback()

      elif msg_type = "RFID":
#        Process_RFID_msg(json_data)         # to put all below in one line
         try: # read the json info
            Car_Name = json_data['Car_Name']
            RXdatetime = datetime.datetime.strptime(json_data['Time'], '%Y-%m-%dT%H:%M:%S')
            UID = json_data['UID']
            Msg_Count = json_data['Msg_Count']
         except: # just get the next message
            continue
         try: # insert info into database
            sql_query = "INSERT INTO RFID_log(Datetime, Car, UID, Msg_Count) \
                         VALUES ('%s', '%s', '%s', '%s')" \
                         % (RXdatetime, Car_Name, UID, Msg_Count)
            cursor.execute(sql_query)
            db.commit()
         except:
            db.rollback()
   else:
      print "==============================================="
      print "====== NON VALID JSON message detected ========"
      print Rx_log_msg
      print "-----------------------------------------------"

finally:
	# close database
	db.close()

	# close the UDP socket
	sLOG.close()
	print "Bye..."

