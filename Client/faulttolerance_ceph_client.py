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
				if (path == msg[1][:len(path)]):
					if msg[2] == "UPDATE:(openc)":
                                                print(path + ' CREATE ' + msg[1][len(path):])

                                        elif msg[2] == "UPDATE:(mkdir)":
                                                print(path + ' CREATE,ISDIR ' + msg[1][len(path):])

                                        elif msg[2] == "UPDATE:(unlink_local)":
                                                print(path + ' DELETE ' + msg[1][len(path):])

                                        #elif msg[3] == "07RMDIR":
                                                #print(path+' DELETE,ISDIR '+msg[2][len(path):])

                                        elif msg[2] == "UPDATE:(rename)":
                                                print(path +' MOVED ' + msg[1][len(path):])

                                        #elif msg[3] == "11CLOSE":
                                                #print(path+' CLOSE '+msg[2][len(path):])

                                        elif msg[2] == "UPDATE:(cap_update)":
                                                print(path+' MODIFY '+msg[1][len(path):])
                                                print(path+' CLOSE '+msg[1][len(path):])

	def signal_handler(self, signum, frame):
                print("Exiting....!!!!!")
                publisher.close()
                context.term()
                sys.exit(0)

if __name__ == '__main__':
	global config
        config = ConfigParser.ConfigParser()
        config.read('config_faulttolerance_ceph_client.ini')
	signal.signal(signal.SIGINT, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGTSTP, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGQUIT, subscribeChangelog().signal_handler)
        signal.signal(signal.SIGTERM, subscribeChangelog().signal_handler)
	logging.basicConfig(filename='log_Worker00.log', level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filemode ='w', maxBytes=5*1024*1024)
        #print = logging.info
        subscribeChangelog().monitor()
