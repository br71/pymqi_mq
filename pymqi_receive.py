import pymqi, sys, os
import configparser
import logging


# This script must be started as mqm user
# Conigure config file
config = configparser.ConfigParser()
config.read("config.ini")
c = config["connection"]

# Config logger
logging.basicConfig(filename='receive_msg.log',level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Read config file
queue_manager = c["queue_manager"]
channel = c["channel"]
host = c["host"]
port = c["port"]
filename = c["filename"]

# Connection options
conn_info = "{}({})".format(host,port)
opts = pymqi.GMO(Options=pymqi.CMQC.MQGMO_SYNCPOINT + pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING)


# Open file
try:
  b = open(filename,"wb")
except Exception as e: 
  logging.debug("Exception occurred", exc_info=True)
  sys.exit(1)
  


# Receive file
try:
  qmgr = pymqi.connect(queue_manager, channel, conn_info)
  q = pymqi.Queue(qmgr, "PYMQI.TEST.1")
  message = q.get(None, None, opts)
  logging.debug("Message sucessfuly received")
except Exception as e: 
  logging.debug("Exception occurred", exc_info=True)
  qmgr.backout()
  logging.debug("Message backout executed")
  qmgr.disconnect()
  sys.exit(1)


# Write message to file
try:
  b.write(message)
  b.close()
  qmgr.commit()
  logging.debug("Changes comitted at MQ, message wtited to file")
  qmgr.disconnect()
except Exception as e: 
  logging.debug("Exception occurred", exc_info=True)
  qmgr.backout()
  logging.debug("Message backout executed")
  qmgr.disconnect()
  b.close()
  os.remove(filename) 
  sys.exit(1)

# No logging option (stack trace to console):
# import traceback
# except:
#     traceback.print_exc() 


# config.ini 
#
# [connection]
# queue_manager = qmanager-name
# channel = channel-name
# host = ip-address
# port = port-number
# filename = name-of-file
# filename_send = name-of-file
