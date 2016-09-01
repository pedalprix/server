#!/usr/bin/env python

# UDP Rx RFID UID
# receives json formatted packets from RFID Tx run on RPi with RFID reader
#    like {"car_name":"","msg_type":"RFID","datetime":"","UID":"[xx,xx,xxx,xx,xx]","rider":"Unknown"}
#    and logs them to a logfile (i.e. many lines of complete json strings)
#
# If UID is same as last read UID it is ignored to limit repeat reads
# To terminate this program:
#     Ctrl-C this program or Tx can send a "" (NULL) message

import socket
import json
import os
import datetime

BIND_IP = "0.0.0.0" # 0.0.0.0 => all interfaces
BIND_PORT = 5550
BIND_ADDR = (BIND_IP, BIND_PORT)

BUFFER_SIZE = 60000

# Initialise UDP server
UDPsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
UDPsock.bind(BIND_ADDR)

# open file
outfilename = "./RFIDtagLogs/Log-config-UID_rider.json." + datetime.datetime.now().strftime('%Y-%m-%d_%H.%M.%S')
outputjson = open(outfilename, 'w')
blob = '{"rfid_UID-rider"' + ":["
print blob

msg_count = 0

LastUID = ""

try:
  Rx_msg, addr = UDPsock.recvfrom(BUFFER_SIZE)
  msg_count += 1
  print "Count     :", msg_count
  print "Rx message1: ", Rx_msg

  while True: # Tx sends a NULL string to terminate session
    print "Before"
    #json_data = json.loads[Rx_msg]
    print "After"

#    if json_data['UID'] == LastUID:
#      pass
#    else:
    blob = blob + "\n   " + Rx_msg.strip() + ","
    Rx_msg, addr = UDPsock.recvfrom(BUFFER_SIZE)
    msg_count += 1
  print "Count     : ", msg_count
  print "Rx message2: ", Rx_msg
  print "Blob follows -------"
  print blob
  print "\nBlob ends -------"

except:
  print "Exception ..."
  pass

finally:
  # close the UDP socket and logfile
  print "Closing UDP Socket and writing blob..."
  UDPsock.close()
  if msg_count > 0:
    blob = blob[0:len(blob)-1] + "\n" # remove comma from last msg
  else:
    blob = blob + '   {"car_name":"","msg_type":"","datetime":"","UID":"[xx,xx,xxx,xx,xx]","rider":"Unknown"}\n'
  blob = blob + "]}"
  print "Final blob follows below:"
  print blob
  outputjson.write(blob)
  outputjson.close()
  print "Done\nBye..."

