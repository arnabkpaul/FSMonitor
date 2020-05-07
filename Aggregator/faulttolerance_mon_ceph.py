from __future__ import print_function
import zmq
import random
from datetime import datetime
import sys
import time
import os
import threading
import signal
import logging
import ConfigParser
import mysql.connector
from Queue import Queue

class subscribeChangelog(object):

	def monitor(self):
		self.receiveChangelogs()

	def receiveChangelogs(self):
		global context
		context = zmq.Context()
		subscriber = context.socket(zmq.SUB)
		global publisher
		publisher = context.socket(zmq.PUB)
		client_count = config.get('MGS', 'Client_Count')
		print(client_count)
		client_ip = config.get('MGS', 'Client_IP').split(",")
		client_port = config.get('MGS', 'Client_Port').split(",")
		for client in range(int(client_count)):
			subscriber.connect("tcp://%s:%s"%(client_ip[client],client_port[client]))
		subscriber.setsockopt(zmq.SUBSCRIBE,'')
		print("connecting to %s:%s"%(client_ip[0],client_port[0]))
		portno = config.get('MGS', 'ZMQ_Publisher_Port')
		publisher.bind("tcp://*:%s"%portno)
		#time.sleep(5)


		while True:
			if int(client_count) == 1:
				msg = subscriber.recv()
                                print ("Received:%s"%(msg))
				if msg:
                                        dates = msg.split(",")
					username = config.get('DB','User')
                			passwd = config.get('DB','Password')
                			hostid = config.get('DB','Host')
                			db = config.get('DB','Database')
                			mydb = mysql.connector.connect(user=username, password=passwd, host=hostid, database=db, auth_plugin='mysql_native_password')
                			cursor = mydb.cursor()
                			print ("Connection established here")
					date1 = dates[0]
                			date2 = dates[1]
                			cursor.execute('SELECT * FROM events WHERE snapshot between %s and %s', (date1, date2))
                			records = cursor.fetchall()
					#f1 = '%H:%M:%S'
                			#f2 = '%Y.%m.%d'
                			for row in records:
                        			rec = "%s,%s,%s,%s"%(row[1],row[2],row[3],row[4])
						print(rec)
						publisher.send_string(rec)
                			cursor.close()
			else:
				for update_nbr in range (int(client_count)):
    					msg = subscriber.recv()
					print(msg)
    					if msg:
						dates = msg.split(",")
                                        	username = config.get('DB','User')
                                        	passwd = config.get('DB','Password')
                                        	hostid = config.get('DB','Host')
                                        	db = config.get('DB','Database')
                                        	mydb = mysql.connector.connect(user=username, password=passwd, host=hostid, database=db, auth_plugin='mysql_native_password')
                                        	cursor = mydb.cursor()
                                        	print ("Connection established 2")
                                        	date1 = dates[0]
                                        	date2 = dates[1]
                                        	cursor.execute('SELECT * FROM events WHERE snapshot between %s and %s', (date1, date2))
                                        	records = cursor.fetchall()
                                        	#f1 = '%H:%M:%S'
                                        	#f2 = '%Y.%m.%d'
                                        	for row in records:
                                                	rec = "%s,%s,%s,%s"%(row[1],row[2],row[3],row[4])
							publisher.send(rec)
                                        	cursor.close()

	def signal_handler(self, signum, frame):
        	print("Exiting....!!!!!")
        	publisher.close()
        	context.term()
        	sys.exit(0)
	
if __name__ == '__main__':
	global config
        config = ConfigParser.ConfigParser()
        config.read('config_faulttolerance_mgs.ini')
	signal.signal(signal.SIGINT, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGTSTP, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGQUIT, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGTERM, subscribeChangelog().signal_handler)
	logging.basicConfig(filename='log_MGS.log', level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filemode ='w', maxBytes=5*1024*1024)
        #print = logging.info
	subscribeChangelog().monitor()
