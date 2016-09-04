#!/usr/bin/env python

####################################################
# DB-Process.py

# This script processes the GPS and RFID log database and generates lap data as it detects GPS co-ordinates crossing start/finish line
# The scripts regularly looks at he 
# ProcessedTime holed the value of time that the 


import MySQLdb
import json
import sys
import math

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

# A*A + B*B = C*C is formula for circle where A and B are othagonal and C is radius.
def DistanceBetween(Location1, Location2):
   diffLongitude = Location1[0] - Location2[0]
   diffLatitude = Location1[1] - Location2[1]
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
# GPS Co-Ordinates are (Longitude, Latitude)
GPS_Finish_Line =         (-35.121015, 139.284525)
GPS_Pit_Lane_Start =      (-35.123919, 139.286669)
GPS_Pit_Lane_End =        (-35.123024, 139.286734)
GPS_Pits =                (-35.123620, 139.286670)


while True:
  # Get all rows from GPS_Log where D_Proc_Flag = FALSE
  sql = "SELECT entry_id, Car_Longitude, Car_Latitude \
         FROM GPS_Log \
         WHERE D_Proc_Flag=FALSE;"
  cursor.execute(sql)
  if cursor.rowcount: # Something to do
    rows = cursor.fetchall()
    for row in rows:
      entry_id = row[0]
      Car_Location = (row[1],row[2])

      D_Finish_Line = DistanceBetween(GPS_Finish_Line, Car_Location)
      D_Pit_Lane_Start = DistanceBetween(GPS_Pit_Lane_Start, Car_Location)
      D_Pit_Lane_End = DistanceBetween(GPS_Pit_Lane_End, Car_Location)
      D_Pits = DistanceBetween(GPS_Pits, Car_Location)
      
      sql = "UPDATE GPS_Log \
             SET D_Proc_Flag=TRUE, \
                 D_Finish_Line=" + str(D_Finish_Line) + ", \
                 D_Pit_Lane_Start=" + str(D_Pit_Lane_Start) + ", \
                 D_Pit_Lane_End=" + str(D_Pit_Lane_End) + ", \
                 D_Pits=" + str(D_Finish_Line) + " \
             WHERE entry_id=" + str(entry_id) + ";"
      print "SQL : ", sql
      SendToSQL(sql)
  else: # No unprocessed rows
    pass



