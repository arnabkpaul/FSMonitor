from __future__ import print_function
import zmq
import random
import sys
import time
import os
import threading
import ConfigParser
import logging
import signal

class subscribeChangelog(object):

        def monitor(self):
		self.receiveChangelogs()

        def receiveChangelogs(self):
                global context
		context = zmq.Context()
                subscriber = context.socket(zmq.SUB)
		mgs_ip = config.get('Client', 'MGS_IP')
		mgs_port = config.get('Client', 'MGS_Port')
                subscriber.connect("tcp://%s:%s"%(mgs_ip,mgs_port))
                subscriber.setsockopt(zmq.SUBSCRIBE,'')
		
		date = str(config.get('Client', 'Date1'))+','+str(config.get('Client', 'Date2'))
		portno = config.get('Client', 'ZMQ_Publisher_Port')
                global publisher
                publisher = context.socket(zmq.PUB)
		publisher.bind("tcp://*:%s"%portno)
		print("sending %s"%(date))
		time.sleep(5)
		publisher.send(date)
		path = config.get('Client', 'Notify_path')
                while True:			
                        msg = subscriber.recv()
                        if msg:
				msg = msg.split(",")
				if (path == msg[2][:len(path)]):
					if msg[3] in ("01CREAT", "02MKDIR", "05MKNOD"):
						print(path+' CREATE '+msg[2][len(path):])
					elif msg[3] in ("06UNLNK", "07RMDIR"):
                                                print(path+' DELETE '+msg[2][len(path):])

					elif msg[3] == "08RENME":
                                                print(path+' MOVED_FROM /'+msg[4]+path+' MOVED_TO '+msg[2][len(path):])

					elif msg[3] == "11CLOSE":
                                                print(path+' CLOSE '+msg[2][len(path):])

					elif msg[3] in ("17MTIME", "18CTIME"):
                                                print(path+' MODIFY '+msg[2][len(path):])

	def signal_handler(self, signum, frame):
                print("Exiting....!!!!!")
                publisher.close()
                context.term()
                sys.exit(0)

if __name__ == '__main__':
	global config
        config = ConfigParser.ConfigParser()
        config.read('config_faulttolerance_client.ini')
	signal.signal(signal.SIGINT, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGTSTP, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGQUIT, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGTERM, subscribeChangelog().signal_handler)
	logging.basicConfig(filename='log_Worker00.log', level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filemode ='w', maxBytes=5*1024*1024)
        #print = logging.info
        subscribeChangelog().monitor()
