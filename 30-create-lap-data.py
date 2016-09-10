#!/usr/bin/env python

####################################################
# 30-create-lap-data.py

import MySQLdb 
import json
import sys
import time
import math
import datetime

configfilename = "Log-config.json"


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

SQL_Host = json_data['SQL_Host']
SQL_User = json_data['SQL_User']
SQL_Passwd = json_data['SQL_Passwd']
SQL_DB = json_data['SQL_DB']

# connect to database
try:
   db = MySQLdb.connect(host=SQL_Host,     # your host, usually localhost
                     user=SQL_User,     # your username
                     passwd=SQL_Passwd, # your password
                     db=SQL_DB)         # name of the data base
   cursor = db.cursor() 
   # cursor = db.cursor(mdb.cursors.DictCursor) to refer to results of fetch by name rater than index
except:
   print "TEST=> ERROR Connecting to database"
   sys.exit()


def SendToSQL(SQLcmd):
   status = 0
   try:
      cursor.execute(SQLcmd)
      db.commit()
   except:
      print "ERROR executing SQL command : ", SQLcmd
      # Rollback in case there is any error
      db.rollback()
      status = -1
   return status

#================================================================
def Get_Rider(UID):
   global cursor
   sql = "SELECT rider \
          FROM RFID_UID \
          WHERE tag_id='" + str(UID) + "' AND rider<>'Unknown' AND rider IS NOT NULL \
          LIMIT 1;"
   cursor.execute(sql)
   if cursor.rowcount != 0:
      row = cursor.fetchone()
      retStr = str(row[0])
   else:
      retStr = UID  # will display UID for rider on monitor when UID not set
   return retStr

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
timestr = '2016-06-19 12:00:00'
racestart = datetime.datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S')

last_entry_id = 0
cars = ("NHS", "NPS")

while True:

  for car in cars:
    prevrow = (0, car, racestart)

    # BEGIN -> Get all rows with Finish_Line = 1
    sql = """SELECT entry_id,
                    Car_Name,
                    GPSdatetimeACST
             FROM GPS_Log
             WHERE Finish_Line IS TRUE
             ORDER BY GPSdatetimeACST ASC;"""
    cursor.execute(sql)
    laps = cursor.rowcount
    rows = cursor.fetchall()
    for row in rows:
      print "row :", row
      End_Time = row[2]
      Lap_Time = row[2] - prevrow[2]
      print "----------------------------------"
      print "prevrow[2] :", prevrow[2]
      print "End_Time :", End_Time
      print "Lap_Time :", Lap_Time
      Rider = Get_Rider_At_Time(End_Time)
      # INSERT new entries into lap database
      sql = "INSERT INTO SF_Laps \
             (Car_Name, End_Time, Lap_Time, Rider) \
             VALUES ('%s', '%s', '0000-00-00 %s', '%s' )" % \
             (car, End_Time, Lap_Time, Rider)
      print sql
      SendToSQL(sql)
      prevrow = row
  time.sleep(2)



