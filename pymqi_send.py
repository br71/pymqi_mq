import pymqi, sys, os
import configparser
import logging


# This script must be started as mqm user
# Conigure config file
config = configparser.ConfigParser()
config.read("config.ini")
c = config["connection"]

# Config logger
logging.basicConfig(filename='send_msg.log',level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Read config file
queue_manager = c["queue_manager"]
channel = c["channel"]
host = c["host"]
port = c["port"]
filename = c["filename_send"]

# Connection options
conn_info = "{}({})".format(host,port)


try:
  qmgr = pymqi.connect(queue_manager, channel, conn_info)
except Exception as e: 
  logging.debug("Exception occurred", exc_info=True)
  sys.exit(1)


try:
  with open(filename, "rb") as b:
    bo = b.read()
except Exception as e: 
  logging.debug("Exception occurred", exc_info=True)

  sys.exit(1)


try:
  q = pymqi.Queue(qmgr, "PYMQI.TEST.1")
  q.put(bo)
  qmgr.commit()
  logging.debug("Message sent and comitted")
  qmgr.disconnect()
except Exception as e: 
  logging.debug("Exception occurred", exc_info=True)
  logging.debug("Unable to send message, exception")
  qmgr.disconnect()
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