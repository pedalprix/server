#!/usr/bin/env python

####################################################
# DB-Create.py

# This script processes the GPS and RFID log database and generates lap data as it detects GPS co-ordinates crossing start/finish line
# The scripts regularly looks at he 
# ProcessedTime holed the value of time that the 


import MySQLdb
import json
import sys
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

print "SQL_Host : ", SQL_Host
print "SQL_User : ", SQL_User
print "SQL_Passwd : ", SQL_Passwd

# connect to database
try:
   db = MySQLdb.connect(host=SQL_Host,  # your host, usually localhost
                        user=SQL_User,     # your username
                        passwd=SQL_Passwd, # your password
                        db='NewDB')          # name of the data base

   print "Connected"
   cursor = db.cursor()
except:
   print "TEST=> ERROR Connecting to database"
   sys.exit()


def SendToSQL(sql):
   status = 0
   sql = re.sub( '\s+', ' ', sql).strip()
   try:
      cursor.execute(sql)
      db.commit()
      print "ALLOK executing SQL command : ", sql
   except:
      print "ERROR executing SQL command : ", sql
      # Rollback in case there is any error
      db.rollback()
      status = -1
   return status


#================================================================
def Drop_NewDB():
   sql = "DROP DATABASE NewDB IF EXISTS;"
   SendToSQL(sql)
   return

def Create_NewDB():
   sql = "CREATE DATABASE NewDB;"
   SendToSQL(sql)
   return
#================================================================
# Cannot test above two def's
#================================================================
def Drop_Table(table):
   sql = "DROP TABLE " + table + ";"
   SendToSQL(sql)
   return

#================================================================
def Create_RFID_UID_table():
   sql = """CREATE TABLE RFID_UID(
             entry_id      INT AUTO_INCREMENT NOT NULL,
             tag_id        TEXT,
             rider         TEXT,
             PRIMARY KEY (entry_id));"""

   SendToSQL(sql)
   return

#================================================================
def Create_RFID_Log_table():
   sql = """CREATE TABLE RFID_Log(
             entry_id        INT NOT NULL AUTO_INCREMENT,
             datetimeACST    DATETIME,
             Car_Name        TEXT,
             Riders_UID      TEXT,
             Msg_Count       INT,
             Car_IP          TEXT,
             PRIMARY KEY (entry_id));"""
   SendToSQL(sql)
   return

#================================================================
def Create_GPS_Log_table():
   sql = """CREATE TABLE GPS_Log(
             entry_id          INT NOT NULL AUTO_INCREMENT,
             datetimeACST      DATETIME,
             Car_Name          TEXT,
             GPSdatetimeACST   DATETIME,
             Car_Latitude      DOUBLE,
             Car_Logitude      DOUBLE,
             Car_Speed         FLOAT,
             D_Proc_Flag       BOOLEAN NOT NULL DEFAULT FALSE,
             D_Finish_Line     DOUBLE,
             D_Pit_Lane_Start  DOUBLE,
             D_Pit_Lane_End    DOUBLE,
             D_Pits            DOUBLE,
             Car_IP            TEXT,
             Msg_Count         INT,
             PRIMARY KEY (entry_id));"""
   SendToSQL(sql)
   return

#================================================================
def Create_DD_Sig_table():
   # This table will have one computed entry for each entry in GPS_Log
   # Puts a TRUE in entry for SIG when DD_* is a min each lap
   sql = "CREATE TABLE DD_Sig( \
             entry_id        INT NOT NULL AUTO_INCREMENT, \
             datetimeACST    DATETIME, \
             Car_Name        TEXT, \
             Car_Latitude    DOUBLE, \
             Car_Logitude    DOUBLE, \
             Car_Speed       FLOAT, \
             Pos1_DD         DOUBLE, \
             Pos1_Sig        BOOLEAN NOT NULL DEFAULT FALSE, \
             Pos1_Sig_ORide  BOOLEAN NOT NULL DEFAULT FALSE, \
             Pos1_Sig_ORVal  BOOLEAN NOT NULL DEFAULT FALSE, \
             PRIMARY KEY (entry_id));"
   SendToSQL(sql)
   return

#================================================================

###################################
## Testing follows:
###################################

Drop_Table("RFID_UID")
Create_RFID_UID_table()
Drop_Table("RFID_Log")
Create_RFID_Log_table()
Drop_Table("GPS_Log")
Create_GPS_Log_table()


