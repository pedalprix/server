#!/usr/bin/env python

####################################################
# DB-Process.py

# This script processes the GPS and RFID log database and generates lap data as it detects GPS co-ordinates crossing start/finish line
# The scripts regularly looks at he 
# ProcessedTime holed the value of time that the 


import MySQLdb 
import json
import sys
import time
import math

configfilename = "Log-config.json"

def beep():
    print "\a"

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

# A*A + B*B = C*C is formula for circle where A and B are othagonal and C is radius.
def DistanceBetween(Location1, Location2):
   diffLatitude = Location1[0] - Location2[0]
   diffLongitude = Location1[1] - Location2[1]
   distance = math.sqrt((diffLongitude*diffLongitude)+(diffLatitude*diffLatitude))
   return distance

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

# THESE NUMBERS NEED TO BE RECHECKED ON SITE FOR MB = Murray Bridge
# GPS Co-Ordinates are (Latitude, Longitude)
GPS_Finish_Line =         (-34.929986, 138.620138)
GPS_Pit_Lane_Start =      (-35.123919, 139.286669)
GPS_Pit_Lane_End =        (-35.123024, 139.286734)
GPS_Pits =                (-35.123620, 139.286670)

Start_100Meters = (-34.929214, 138.577813)
Finish_100Meters = (-34.929916, 138.578308)
_105m = 0.000859
_1m = _105m / 105.0

Limit_Distance = 30.0

while True:
  # Get all rows from GPS_Log where D_Finish_Line IS NULL OR D_Pit_Lane_Start IS NULL OR D_Pit_Lane_End IS NULL OR D_Pits IS NULL
  sql = "SELECT entry_id, Car_Latitude, Car_Longitude, Car_Speed \
         FROM GPS_Log \
         WHERE D_Finish_Line IS NULL OR D_Pit_Lane_Start IS NULL OR D_Pit_Lane_End IS NULL OR D_Pits IS NULL;"
  cursor.execute(sql)
  if cursor.rowcount: # Something to do
    rows = cursor.fetchall()
    for row in rows:
      D_Proc_Flag = False
      entry_id = row[0]
      Car_Location = (row[1],row[2])
      Car_Speed = row[3]
      D_Finish_Line = DistanceBetween(GPS_Finish_Line, Car_Location)
      D_Pit_Lane_Start = DistanceBetween(GPS_Pit_Lane_Start, Car_Location)
      D_Pit_Lane_End = DistanceBetween(GPS_Pit_Lane_End, Car_Location)
      D_Pits = DistanceBetween(GPS_Pits, Car_Location)

      D_Finish_Line = D_Finish_Line / _1m
      D_Pit_Lane_Start = D_Pit_Lane_Start / _1m
      D_Pit_Lane_End = D_Pit_Lane_End / _1m
      D_Pits = D_Pits/ _1m

#      astring = "D_Finish_Line :" + str(D_Finish_Line) + "  Limit_Distance " + str(Limit_Distance)
#      print astring
      if D_Finish_Line < Limit_Distance:
         print "Distance within limit of finish line at entry_id : ", entry_id
         D_Proc_Flag = True

         # BEGIN: CODE TO SET D_ProcFlag True when closest to finish line.
         # Get previous entry
         sql = "SELECT entry_id, D_Proc_Flag, D_Finish_Line FROM GPS_Log WHERE entry_id=" + str(entry_id-1) + ";"
         cursor.execute(sql)
         if cursor.rowcount:
            prevRow=cursor.fetchone()
            print "Previous row : ", prevRow
            prev_entry_id = prevRow[0]
            prev_D_Proc_Flag = prevRow[1]
            prev_D_Finish_Line = prevRow[2]
         else:
            prev_entry_id = None
            prev_D_Proc_Flag = False
            prev_D_Finish_Line = 100.0

         if D_Finish_Line <= prev_D_Finish_Line: # Current is closer than previous
            # Clear previous entry's D_Proc_Flag
            print " Clearing D_Proc_Flag for entry_id : ", prev_entry_id
            sql = "UPDATE GPS_Log SET D_Proc_Flag=False WHERE entry_id=" + str(prev_entry_id) + ";"
            SendToSQL(sql)
            sql = "SELECT entry_id, D_Proc_Flag FROM GPS_Log WHERE entry_id=" + str(prev_entry_id) + ";"
            cursor.execute(sql)
            print "Result : ", cursor.fetchone()

         else: # Previous is closer than current
            D_Proc_Flag=False
         # END: NEW CODE TO SET D_ProcFlag True when closest to finish line.

      # Update database
      sql = "UPDATE GPS_Log \
             SET D_Proc_Flag=" + str(D_Proc_Flag) + ", \
                 D_Finish_Line=" + str(D_Finish_Line) + ", \
                 D_Pit_Lane_Start=" + str(D_Pit_Lane_Start) + ", \
                 D_Pit_Lane_End=" + str(D_Pit_Lane_End) + ", \
                 D_Pits=" + str(D_Pits) + " \
             WHERE entry_id=" + str(entry_id) + ";"
#      print "SQL : ", sql
      SendToSQL(sql)
  else: # No unprocessed rows
    print "No unprocessed rows"
    pass
  time.sleep(2)



