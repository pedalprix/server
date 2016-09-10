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

# A*A + B*B = C*C is formula for circle where A and B are othagonal and C is radius.
def DistanceBetween(Location1, Location2):
   global _1m
   diffLatitude = Location1[0] - Location2[0]
   diffLongitude = Location1[1] - Location2[1]
   distance = math.sqrt((diffLongitude*diffLongitude)+(diffLatitude*diffLatitude)) / _1m
   return distance

# http://andrew.hedges.name/experiments/haversine/
def AnotherDistanceBetween(Location1, Location2):
   R = 6373.0
   lat1 = Location1[0]
   lat2 = Location2[0]
   lon1 = Location1[1]
   lon2 = Location2[1]
   dlon = lon2 - lon1
   dlat = lat2 - lat1 
   a = (math.sin(dlat/2))**2 + math.cos(lat1) * math.cos(lat2) * (math.sin(dlon/2))**2
   c = 2.0 * math.atan2( math.sqrt(a), math.sqrt(1-a) )
   d = R * c    # (where R is the radius of the Earth) 
   distance = d
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

#######################################################################################
while True:
  # Get most recent processed row
  sql = """SELECT entry_id, 
                  Car_Latitude,
                  Car_Longitude,
                  Car_Speed,
                  GPSdatetimeACST
           FROM GPS_Log
           WHERE D_Finish_Line IS NOT NULL
             AND D_Pit_Lane_Start IS NOT NULL
             AND D_Pit_Lane_End IS NOT NULL 
             AND D_Pits IS NOT NULL
           ORDER BY GPSdatetimeACST DESC
           LIMIT 1;"""
  cursor.execute(sql)
  row = cursor.fetchone()
  last_processed_time = row[4]
  last_processed_entry_id = row[0]

  # Get all unprocessed rows
  sql = """SELECT entry_id, 
                  Car_Latitude,
                  Car_Longitude,
                  Car_Speed,
                  GPSdatetimeACST
           FROM GPS_Log
           WHERE D_Finish_Line IS NULL
              OR D_Pit_Lane_Start IS NULL
              OR D_Pit_Lane_End IS NULL 
              OR D_Pits IS NULL
           ORDER BY GPSdatetimeACST ASC;"""
  cursor.execute(sql)
  if cursor.rowcount: # New GPS_Log entries exist
    rows = cursor.fetchall()

    # TODO : Check if oldest unprocessed GPSdatetimeACST is greater than newest processed GPSdatetimeACST
    #        otherwise clear all D_data and continue with next iteration
    #        which will restart processing from race start.
    #        This is only needed if log packets received out of order

    for row in rows:
      Finish_Line = False
      entry_id = row[0]
      Car_Location = (row[1],row[2])
      Car_Speed = row[3]

      D_Finish_Line    = DistanceBetween(Car_Location, GPS_Finish_Line)
      D_Pit_Lane_Start = DistanceBetween(Car_Location, GPS_Pit_Lane_Start)
      D_Pit_Lane_End   = DistanceBetween(Car_Location, GPS_Pit_Lane_End)
      D_Pits           = DistanceBetween(Car_Location, GPS_Pits)

#      astring = "D_Finish_Line :" + str(D_Finish_Line) + "  Limit_Distance " + str(Limit_Distance)
#      print astring
  
      if D_Finish_Line < Limit_Distance:  # We are close to the finish line
         Finish_Line = True

         # Now look for the minimum D_Finish_Line ie when Car_Location is closest to GPS_Finish_Line

         # Get previous entry to compare against
         sql = "SELECT entry_id, Finish_Line, D_Finish_Line FROM GPS_Log WHERE entry_id=" + str(entry_id-1) + ";"
         cursor.execute(sql)
         if cursor.rowcount:
            prevRow=cursor.fetchone()
            prev_entry_id = prevRow[0]
            prev_Finish_Line = prevRow[1]
            prev_D_Finish_Line = prevRow[2]
         else: # We must be at the first entry in the GPS_Log
            prev_entry_id = None
            prev_Finish_Line = False
            prev_D_Finish_Line = 100.0

         # Compare current entry with previous entry
         if D_Finish_Line <= prev_D_Finish_Line: # Current is closer than previous
            # Clear previous entry's Finish_Line
            sql = "UPDATE GPS_Log SET Finish_Line=False WHERE entry_id=" + str(prev_entry_id) + ";"
            SendToSQL(sql)

         else: # Previous is closer than current so moving away
            Finish_Line=False

         # END: NEW CODE TO SET D_ProcFlag True when closest to finish line.

      # Update database
      sql = "UPDATE GPS_Log \
             SET Finish_Line=" + str(Finish_Line) + ", \
                 D_Finish_Line=" + str(D_Finish_Line) + ", \
                 D_Pit_Lane_Start=" + str(D_Pit_Lane_Start) + ", \
                 D_Pit_Lane_End=" + str(D_Pit_Lane_End) + ", \
                 D_Pits=" + str(D_Pits) + " \
             WHERE entry_id=" + str(entry_id) + ";"
#      print "SQL : ", sql
      SendToSQL(sql)
  else:
    print "No unprocessed rows"
    pass
  time.sleep(2)



