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

JSON_Process_error_Cnt = 0

# process received packets
try:
  while True:
    Rx_log_msg, addr = sLOG.recvfrom(LOG_BUFFER_SIZE)
    Car_IP = addr[0]
    Car_PORT = addr[1]

    if is_json(Rx_log_msg):
      json_data = json.loads(Rx_log_msg)
      msg_type = json_data['Msg_Type']

      # GPS
      if msg_type == "GPS":
         try: # read the json info
            datetimeACST = str(datetime.datetime.strptime(json_data['TimeACST'], '%Y-%m-%d %H:%M:%S'))
            Msg_Count = int(json_data['Msg_Count'])
            Car_Name = json_data['Car_Name']
            GPSdatetimeUTC = str(datetime.datetime.strptime(json_data['Msg'][0]['tpv'][0]['time'], '%Y-%m-%dT%H:%M:%S.%fZ')) #"2016-06-19T01:50:42.000Z"
            GPSdatetimeACST = GPSdatetimeUTC + datetime.timedelta(hours=9,minutes=30)
            Car_Latitude = float(json_data['Msg'][0]['tpv'][0]['lat'])
            Car_Longitude = float(json_data['Msg'][0]['tpv'][0]['lon'])
            Car_Speed_mps = float(json_data['Msg'][0]['tpv'][0]['speed'])
            Car_Speed = Car_Speed_mps * 3.6 # meters per second to kilometers per hour

         except: # just get the next message
            JSON_Process_error_Cnt += 1
            print "----------------------------------------------------------------------------------------"
            print "ERROR processing JSON fields in GPS message. JSON_Process_error_Cnt : ",JSON_Process_error_Cnt
            if JSON_Process_error_Cnt % 10 == 0:
               print "----------------------------------------------------------------------------------------"
               print json_data
               print "----------------------------------------------------------------------------------------"
            try:
               datetimeACST = str(datetime.datetime.strptime(json_data['TimeACST'], '%Y-%m-%d %H:%M:%S'))
               print "datetimeACST:",datetimeACST
               Msg_Count = int(json_data['Msg_Count'])
               print "Msg_Count:",Msg_Count
               Car_Name = json_data['Car_Name']
               print "Car_Name:",Car_Name
               GPSdatetimeUTC = str(datetime.datetime.strptime(json_data['Msg'][0]['tpv'][0]['time'], '%Y-%m-%dT%H:%M:%S.%fZ')) #"2016-06-19T01:50:42.000Z"
               print "GPSdatetimeUTC:",GPSdatetimeUTC
               GPSdatetimeACST = GPSdatetimeUTC + datetime.timedelta(hours=9,minutes=30)
               print "GPSdatetimeACST:",GPSdatetimeACST
               Car_Latitude = float(json_data['Msg'][0]['tpv'][0]['lat'])
               print "Car_Latitude:",Car_Latitude
               Car_Longitude = float(json_data['Msg'][0]['tpv'][0]['lon'])
               print "Car_Longitude:",Car_Longitude
               Car_Speed_mps = float(json_data['Msg'][0]['tpv'][0]['speed'])
               print "Car_Speed_mps:",Car_Speed_mps
               Car_Speed = Car_Speed_mps * 3.6 # meters per second to kilometers per hour
               print "Car_Speed:",Car_Speed
            except:
               pass


            continue

         try: # insert info into database
            sql_query = "INSERT INTO GPS_Log(datetimeACST, Msg_Count, Car_Name, GPSdatetimeACST, Car_Longitude, Car_Latitude, Car_Speed, Car_IP) \
                         VALUES             ('%s',         %d,        '%s',     '%s',            %f,            %f,           %f,        '%s');" \
                         %                  (datetimeACST, Msg_Count, Car_Name, GPSdatetimeACST, Car_Longitude, Car_Latitude, Car_Speed, Car_IP)
            cursor.execute(sql_query)
            db.commit()
#            print "Message %d added to database from %s with datestamp %s" % (Msg_Count, msg_type, datetimeACST)
         except:
            print "GPS: SQL Exception =============================="
            print spl_query
            print "-------------------------------------------------"
            db.rollback()

      # RFID
      elif msg_type == "RFID":
         try: # read the json info
            datetimeACST = str(datetime.datetime.strptime(json_data['TimeACST'], '%Y-%m-%d %H:%M:%S'))
            Msg_Count = int(json_data['Msg_Count'])
            Car_Name = json_data['Car_Name']
            Riders_UID = json_data['Msg'][0]['RFID-UID']
         except: # just get the next message
            print "ERROR processing JSON fields in RFID message."
            continue

         try: # insert info into database
            sql_query = "INSERT INTO RFID_Log(datetimeACST, Car_Name, Riders_UID, Msg_Count, Car_IP) \
                         VALUES (            '%s',          '%s',     '%s',       %d,        '%s');" \
                         %                   (datetimeACST, Car_Name, Riders_UID, Msg_Count, Car_IP)
            cursor.execute(sql_query)
            db.commit()
#            print "Message %d added to database from %s with datestamp %s" % (Msg_Count, msg_type, datetimeACST)
         except:
            print "RFID: SQL Exception"
            db.rollback()
      else:
        print "==============================================================="
        print "ERROR : INVALID msg_type detected. Received message follows:"
        print Rx_log_msg
        print "---------------------------------------------------------------"
    else:
      print "==============================================="
      print "ERROR : INVALID JSON below"
      print Rx_log_msg
      print "-----------------------------------------------"

finally:
  # close database
  db.close()

  # close the UDP socket
  sLOG.close()
  print "Bye..."

