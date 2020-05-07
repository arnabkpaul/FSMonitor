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
		self.queue = Queue(maxsize=100)
		t1 = threading.Thread(target=self.receiveChangelogs)
		t2 = threading.Thread(target=self.saveChangelogs)
		t1.setDaemon(True)
		t2.setDaemon(True)
		t1.start()
		t2.start()
		t1.join()
		t2.join()

	def receiveChangelogs(self):
		context = zmq.Context()
		subscriber = context.socket(zmq.SUB)
		publisher = context.socket(zmq.PUB)
		mds_count = config.get('MON', 'MDS_Count')
		mds_ip = config.get('MON', 'MDS_IP').split(",")
		mds_port = config.get('MON', 'MDS_Port').split(",")
		#subscriber.connect("tcp://10.0.7.96:5557")
		#for mds in range(int(mds_count)):
			#subscriber.connect("tcp://%s:%s"%(mds_ip[mds],mds_port[mds]))
		subscriber.connect("tcp://192.168.0.192:5557")
		subscriber.setsockopt(zmq.SUBSCRIBE,'')
		portno = config.get('MON', 'ZMQ_Publisher_Port')
		publisher.bind("tcp://*:%s"%portno)
		time.sleep(5)

		#path = '/home/ec2-user/monitoring'
		#filename = 'changelog.txt'
		#destfile = path + '/' + filename

		#if os.path.isfile(destfile):
    			#os.remove(destfile)
    			#time.sleep(2)

		while True:
			if int(mds_count) == 1:
				msg = subscriber.recv()
                                if msg:
                                        #f = open(destfile, 'a+')
                                        #print 'open'
                                        print(msg)
                                        #f.write(msg)
                                        #print 'close\n'
                                        #f.close()
                                        #print('Sending Message to Subscribers')
                                        #publisher.send(msg)
                                        self.queue.put(msg)
                                        #print('Sent msg to queue')
			else:
				for update_nbr in range (int(mds_count)):
    					msg = subscriber.recv()
    					if msg:
        					#f = open(destfile, 'a+')
        					#print 'open'
						print(msg)
        					#f.write(msg)
        					#print 'close\n'
        					#f.close()
						#print('Sending Message to Subscribers')
						#publisher.send(msg)
						self.queue.put(msg)
						#print('Sent msg to queue')	

	def saveChangelogs(self):
		username = config.get('DB','User')
		passwd = config.get('DB','Password')
		hostid = config.get('DB','Host')
		db = config.get('DB','Database')
		mydb = mysql.connector.connect(user=username, password=passwd, host=hostid, database=db, auth_plugin='mysql_native_password')
		cursor = mydb.cursor()
		print ("Connection established")
		while True:
			message = self.queue.get().split(",")
			now = datetime.now()
			formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
			cursor.execute('INSERT INTO events(snapshot, path, event, file) \
          VALUES (%s,%s,%s,%s)', (formatted_date, message[2], message[3], message[4]))
			#logging.info ('Queued Message %s'%(message))
			mydb.commit()
		cursor.close()

	def signal_handler(self, signum, frame):
        	print("Exiting....!!!!!")
        	publisher.close()
        	context.term()
        	sys.exit(0)
	
if __name__ == '__main__':
	global config
        config = ConfigParser.ConfigParser()
        config.read('config_mon_ceph.ini')
	#signal.signal(signal.SIGINT, subscribeChangelog().signal_handler)
        #signal.signal(signal.SIGTSTP, subscribeChangelog().signal_handler)
        #signal.signal(signal.SIGQUIT, subscribeChangelog().signal_handler)
        #signal.signal(signal.SIGTERM, subscribeChangelog().signal_handler)
	#logging.basicConfig(filename='log_MGS.log', level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filemode ='w', maxBytes=5*1024*1024)
        #print = logging.info
	subscribeChangelog().monitor()
