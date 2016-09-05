#!/usr/bin/env python

####################################################
# Tools.py


import MySQLdb
import json
import sys
import datetime
import re

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

#================================================================
def RunSQL(sql):
   global cursor

   print "RunSQL BEGIN ----------------------------"
   sql = re.sub( '\s+', ' ', sql).strip()
   print "SQL Query : ", sql

   try:
      cursor.execute(sql)
      print "SQL executed OK. Rowcount : ", cursor.rowcount
   except:
      print "==> EXCEPTION with cursor.execute()"

   print "RunSQL END ------------------------------"
   return

#================================================================
def Get_UID(rider):
   global cursor
   sql = "SELECT tag_id \
          FROM RFID_UID \
          WHERE rider= '" + str(rider) + "' \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = cursor.fetchone()
      retStr = str(row[0])
   else:
      retStr = None
   return retStr

#================================================================
def Get_Rider(UID):
   global cursor
   sql = "SELECT rider \
          FROM RFID_UID \
          WHERE tag_id='" + str(UID) + "' AND rider<>'Unknown' \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = cursor.fetchone()
      retStr = str(row[0])
   else:
      retStr = None
   return retStr

#================================================================
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
      retStr = str(row[0])
   else:
      retStr = None
   return retStr

#================================================================
def Get_Latest_Rider(Car_Name):
   return Get_Rider(Get_Latest_UID(Car_Name))

#================================================================
def Get_UID_At_Time(time):
   global cursor
   sql = " SELECT Riders_UID \
           FROM RFID_Log \
           WHERE datetimeACST <= '" + str(time) + "' \
           ORDER BY datetimeACST DESC \
           LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount == 1:
      row = cursor.fetchone()
      retStr = str(row[0])
   else:
      retStr = None
   return retStr

#================================================================
def Get_Rider_At_Time(time):
   return Get_Rider(Get_UID_At_Time(time))

#================================================================
def Get_Latest_Car_Latitude(Car_Name):
   global cursor
   sql = "SELECT Car_Latitude \
          FROM GPS_Log \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY GPSdatetimeACST DESC \
          LIMIT 1;"
   cursor.execute(sql)
   print "rowcount : ", cursor.rowcount
   if cursor.rowcount > 0:
      row = cursor.fetchone()
      retStr = float(row[0])
   else:
      retStr = None
   return retStr

#================================================================
def Get_Latest_Car_Longitude(Car_Name):
   global cursor
   sql = "SELECT Car_Longitude \
          FROM GPS_Log \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY GPSdatetimeACST DESC \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = cursor.fetchone()
      retStr = float(row[0])
   else:
      retStr = None
   return retStr

#================================================================
def Get_Latest_RFID_Timestamp(Car_Name):
   global cursor
   sql = "SELECT datetimeACST \
          FROM RFID_Log \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY datetimeACST DESC \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = cursor.fetchone()
      retStr = str(row[0])
   else:
      retStr = None
   return retStr

#================================================================
def Get_Latest_GPS_Timestamp(Car_Name):
   global cursor
   sql = "SELECT GPSdatetimeACST \
          FROM GPS_Log \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY GPSdatetimeACST DESC \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = cursor.fetchone()
      retStr = str(row[0])
   else:
      retStr = None
   return retStr

#================================================================
def Get_N_Fastest_Laps_For_Car(Car_Name,N):
   global cursor
   sql = "SELECT Riders_UID, Lap_Time \
          FROM Lap_data \
          WHERE Car_Name='" + Car_Name + "' \
          ORDER BY datetimeACST DESC \
          LIMIT " + TopN + ";"
   cursor.execute(sql)
   rows = cursor.fetchall()
   return rows


def Get_Fastest_N_Laps_For_UID(UID, N):
   global cursor
   sql = "SELECT Lap_Time \
          FROM Lap_data \
          WHERE Rider_UID='" + UID + "' \
          ORDER BY entry_id DESC \
          LIMIT " + Num + ";"
   cursor.execute(sql)
   rows = cursor.fetchall()
   return rows


def Get_Fastest_N_Laps_For_Rider(Rider_Name, N):
   global cursor
   # Get riders UID
   sql = "SELECT tag_id \
          FROM RFID_UID \
          WHERE rider='" + Rider_Name + "' \
          ORDER BY entry_id DESC \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.count:
     row = cursor.fetchone()
     UID = row[0]
     rows = cursor.fetchall()
     return rows
   else:
     return None

def Get_Last_N_Laps_For_Car(Car_Name,N):
   global cursor

   sql = "SELECT datetimeACST \
          FROM GPS_Log \
          WHERE Car_Name='" + Car_Name + "' AND D_Proc_Flag=1 \
          ORDER BY datetimeACST C \
          LIMIT " + N + ";"
   cursor.execute(sql)
   rows = cursor.fetchall()
   return rows

##########################
### TESTING 
##########################
uid = Get_UID('BenV')
print "Get_UID('BenV') : ", uid

rider = Get_Rider('[136, 4, 140, 3, 3]')
print "Get_Rider('[136, 4, 140, 3, 3]') : ", rider

uid = Get_Latest_UID('TEST')
print "Get_Latest_UID('TEST') : ", uid

rider = Get_Latest_Rider('TEST')
print "Get_Latest_Rider('TEST') : ", rider

timestr = '2016-09-03 04:52:27'
print "dt : ", timestr
dt = datetime.datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S')
uid = Get_UID_At_Time(dt)
print "Get_UID_At_Time(dt) : ", uid

timestr = '2016-09-03 04:52:28'
print "dt : ", timestr
dt = datetime.datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S')
rider = Get_Rider_At_Time(dt)
print "Get_Rider_At_Time(dt) : ", rider

lat = Get_Latest_Car_Latitude('SIM')
print "Get_Latest_Car_Latitude('SIM') : ", lat

lon = Get_Latest_Car_Longitude('SIM')
print "Get_Latest_Car_Longitude('SIM') : ", lon

dtStr = Get_Latest_RFID_Timestamp('TEST')
print "Get_Latest_RFID_Timestamp('TEST') : ", dtStr

dtStr = Get_Latest_GPS_Timestamp('SIM')
print "Get_Latest_GPS_Timestamp('SIM') : ", dtStr


timestr = '2016-09-03 04:52:28'
print "dt : ", timestr
dt = datetime.datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S')
dt1 = 
print "dt + dt : ", dt

