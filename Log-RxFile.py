#!/usr/bin/env python

# Rx UDP json messages and insert into database

import socket
import os
import datetime

LOG_BUFFER_SIZE = 400000
ADDR = ("0.0.0.0", 9999)

# Initialise UDP Log server
print "Binding socket"
sFile = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sFile.bind(ADDR)

print "1"
try:
  while True:
    Rx_blob, addr = sFile.recvfrom(LOG_BUFFER_SIZE)

    print "2"
    RxTime = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%s")
    print "3 ", RxTime
    outfilename = "./RxFiles/RxFILE." + RxTime
    print "outfilename : ", outfilename
    outfile = open(outfilename, "w")
    outfile.write(Rx_blob)
    outfile.close()
except:
  print "Exception captured.."
  pass
finally:
  print "Closing socket..."
  sFile.close()
  print "Bye..."
