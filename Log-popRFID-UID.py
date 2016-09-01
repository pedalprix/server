#!/usr/bin/env python

####################################################
# Log-popRFID-UID.py
# Script to move contents of RFID json to SQL database
#
# To Use:
#   1. Populate a UID_rider.json 
#   2. Call Log-popRFID-UID.py as per below
#
#        USAGE: sudo python Log-popRFID-UID.py [-clear|-append] <filename>
#
#         Eg 1: sudo python Log-popRFID-UID.py Log-config-UID_rider.json
#               => will process Log-config-UID_rider.json WITHOUT clearing database
#
#         Eg 2:  sudo python Log-popRFID-UID.py -clear Log-config-UID_rider.json"
#               => will process Log-config-UID_rider.json AFTER clearing database table"


import MySQLdb
import json
import sys

configfilename = "Log-config.json"

def printusage():
   print "ERROR with command line arguements"
   print "USAGE: sudo python Log-popRFID-UID.py [-clear|-append] <filename>"
   print "   Eg: sudo python Log-popRFID-UID.py Log-config-UID_rider.json"
   print "       => will process Log-config-UID_rider.json WITHOUT clearing database \n"
   print "       sudo python Log-popRFID-UID.py -clear Log-config-UID_rider.json"
   print "       => will process Log-config-UID_rider.json AFTER clearing database table"
   sys.exit()
   return

# Command line arg processing
try:
   # Get args passed in command line
   Clear_Table = False
   if sys.argv[1] == "-clear":
      RFID_RIDER_JSON = sys.argv[2]
      Clear_Table = True
   elif sys.argv[1] == "-append":
      RFID_RIDER_JSON = sys.argv[2]
      Clear_Table = False
   else:
      printusage()
   # Try open and close the filename passed as first command line arg
   dummy = open(RFID_RIDER_JSON, 'r')
   dummy.close()
   print "sys.argv[0] : ", sys.argv[0]
   print "sys.argv[1] : ", sys.argv[1]
   print "sys.argv[2] : ", sys.argv[2]
except:
   printusage()

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
   print "TEST=> OK Connected to database"
except:
   print "TEST=> ERROR Connected to database"
   sys.exit()

# clear RFID table in pp database if requested
if Clear_Table:
   try:
      sql_query = "TRUNCATE TABLE " + SQL_RFID_UID_table_name + ";"
      print "TEST=> SQL : ", sql_query
      cursor.execute(sql_query)
      print "Post execute : "
      db.commit()
      print "CLEARING TABLE ", SQL_RFID_UID_table_name
   except:
      db.rollback()
      print "==> ERROR clearing table ", SQL_RFID_UID_table_name

# open config file
file = open(RFID_RIDER_JSON, 'r')
data = file.read()
file.close()

try:
   json_data = json.loads(data)
   index = 0;

   while True:
      try:
         tag_id = json_data['rfid_UID-rider'][index]['UID']
         rider = json_data['rfid_UID-rider'][index]['rider']
         print "\nTEST=> : ", index, tag_id, rider
      except:
         print "End of valid list. Valid entries : ",index
         break

      # Check if UID is in SQL_RFID-UID_table_name
      sql_query = "SELECT entry_id,rider FROM " + SQL_RFID_UID_table_name + ' WHERE tag_id="' + tag_id + '";'
      print "TEST=> 1.1 SQL : ", sql_query
      cursor.execute(sql_query)
      Found = cursor.rowcount
      print "TEST=> 1.2 Found : ", Found


      if Found: # At least one entry exists for UID
         if rider != "Unknown":
            print "TEST=> 2.1 New rider : ", rider
            rows = cursor.fetchall()
            for row in rows:
               print "TEST=> row[0] :", row[0]
               print "TEST=> row[1] :", row[1]
               if row[1] == "Unknown":
                  print "TEST=> 3.1  "
                  sql_query = 'UPDATE ' + SQL_RFID_UID_table_name
                  print "TEST=> 3.2 SQL : ", sql_query
                  sql_query += ' SET rider=' + '"' + rider + '"'
                  print "TEST=> 3.3 SQL : ", sql_query
                  sql_query += ' WHERE entry_id=' + str(row[0]) + ';'
                  print "TEST=> 3.4 SQL : ", sql_query
                  cursor.execute(sql_query)
                  db.commit()

      else: # not found so insert into database
         sql_query = "INSERT INTO " + SQL_RFID_UID_table_name + "(tag_id, rider) VALUES ('%s', '%s')" % (tag_id, rider) + ";"
         cursor.execute(sql_query)
         db.commit()

      index += 1

except:
   print "EXCEPTION Detected."
   db.rollback()

finally:
   # print out the database
   print "Records read : ", index
   print "======================================="
   print "--- Database after changes ------------"
   sql_query = "SELECT entry_id,tag_id,rider FROM " + SQL_RFID_UID_table_name + ";"
   cursor.execute(sql_query)

   rows = cursor.fetchall()
   print "entry_id, tag_id, rider"
   for row in rows:
      print str(row[0]) + " " + str(row[1]) + "  " + str(row[2])

   print "Closing database"
   db.close()
   print "Bye..."
