#!/usr/bin/env python

####################################################
# Tools.py


import MySQLdb
import json
import sys

configfilename = "Log-config.json"

def printusage():
   sys.exit()
   return

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return True

# open configfilename and read
configfile = open(configfilename, 'r')
configfilejson = configfile.read()
configfile.close()

# parse configfilejson
json_data = json.loads(configfilejson)

LOG_IP = json_data['LOG_IP']
LOG_PORT = int(json_data['LOG_PORT'])
LOG_BUFFER_SIZE = int(json_data['LOG_BUFFER_SIZE'])

SQL_Host = json_data['SQL_Host']
SQL_User = json_data['SQL_User']
SQL_Passwd = json_data['SQL_Passwd']
SQL_DB = json_data['SQL_DB']
SQL_RFID_UID_table_name = json_data['SQL_RFID_UID_table_name']

LOG_ADDR = (LOG_IP, LOG_PORT)


# connect to database
try:
   db = MySQLdb.connect(host=SQL_Host,     # your host, usually localhost
                     user=SQL_User,     # your username
                     passwd=SQL_Passwd, # your password
                     db=SQL_DB)         # name of the data base

   cursor = db.cursor()
except:
   print "TEST=> ERROR Connecting to database"
   sys.exit()


def Get_Rider(UID):
   global cursor
   sql = "SELECT rider \
          FROM RFID_UID \
          WHERE tag_id='" + str(UID) + "' AND rider<>'Unknown' \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = str(cursor.fetchone())
      retStr = row[0]
   else:
      retStr = "Unknown"
   return retStr

def Get_Latest_UID(Car_Name):
   global cursor
   sql = "SELECT Riders_UID \
          FROM RFID_Log \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY datetimeACST DESC \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount == 1:
      row = cursor.fetchone()
      retStr = row[0]
   else:
      retStr = ""
   return retStr

def Get_Latest_Rider(Car_Name):
   return Get_Rider(Get_Latest_UID(Car_Name))

def Get_Latest_Car_Latitude(Car_Name):
   global cursor
   sql = "SELECT Car_Latitude \
          FROM GPS_Log \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY datetimeACST DESC \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount > 0:
      row = cursor.fetchone()
      retStr = float(row[0])
   else:
      retStr = 0.0
   return retStr

def Get_Latest_Car_Longitude(Car_Name):
   global cursor
   sql = "SELECT Car_Longitude \
          FROM GPS_Log \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY datetimeACST DESC \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = cursor.fetchone()
      retStr = float(row[0])
   else:
      retStr = 0.0
   return retStr

print "Current Latitude in TEST : ", Get_Latest_Car_Latitude("TEST")
