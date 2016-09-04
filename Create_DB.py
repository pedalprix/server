#!/usr/bin/env python

####################################################
# DB-Create.py

# This script processes the GPS and RFID log database and generates lap data as it detects GPS co-ordinates crossing start/finish line
# The scripts regularly looks at he 
# ProcessedTime holed the value of time that the 


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


def Create_Blank_DB():
   # Script to run on the Amazon server
   # 1. Create database
   sql = "CREATE DATABASE " + SQL_DB + ";"
   SendToSQL(sql)   

   # 2. Create RFID_UID table
   sql = "CREATE TABLE RFID_UID( \
             entry_id        INT NOT NULL AUTO_INCREMENT, \
             tag_id          TEXT, \
             rider           TEXT, \
             PRIMARY KEY (entry_id));"
   SendToSQL(sql)   

   # 3. Create RFID_Log table
   sql = "CREATE TABLE RFID_Log( \
             entry_id        INT NOT NULL AUTO_INCREMENT, \
             datetimeACST    DATETIME, \
             Car_Name        TEXT, \
             Car_IP          TEXT, \
             Riders_UID      TEXT, \
             Msg_Count       INT, \
             PRIMARY KEY (entry_id));"
   SendToSQL(sql)

   # 4. Create GPS_Log table
   sql = "CREATE TABLE GPS_Log( \
             entry_id        INT NOT NULL AUTO_INCREMENT, \
             datetimeACST    DATETIME, \
             Car_Name        TEXT, \
             Car_IP          TEXT, \
             Msg_Count       INT, \
             Car_Latitude    DOUBLE, \
             Car_Logitude    DOUBLE, \
             Car_Speed       FLOAT, \
             D_Proc_Flag       BOOLEAN NOT NULL DEFAULT FALSE, \
             D_Finish_Line     DOUBLE, \
             D_Pit_Lane_Start  DOUBLE, \
             D_Pit_Lane_End    DOUBLE, \
             D_Pits            DOUBLE, \
             PRIMARY KEY (entry_id));"
   SendToSQL(sql)

   # 5. Create DD_Sig table
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
