from __future__ import print_function
import zmq
import random
import sys
import time
import os
import threading
import ConfigParser
import logging
from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler  

class subscribeChangelog(object):

        def monitor(self):
			type = config.get('Client', 'Type')
			if type == 'Lustre':
                t1 = threading.Thread(target=self.receiveChangelogs)
                t1.setDaemon(True)
                t1.start()
                t1.join()
			elif type in ('Linux','MAC'):
                t1 = threading.Thread(target=self.receiveUnix)
                t1.setDaemon(True)
                t1.start()
                t1.join()

        def receiveChangelogs(self):
                context = zmq.Context()
                subscriber = context.socket(zmq.SUB)
				mgs_ip = config.get('Client', 'MGS_IP')
				mgs_port = config.get('Client', 'MGS_Port')
                subscriber.connect("tcp://%s:%s"%(mgs_ip,mgs_port))
                subscriber.setsockopt(zmq.SUBSCRIBE,'')

                #path = '/home/ec2-user/lustre_ripple_worker00/monitoring_worker00'
                #filename = 'changelog.txt'
                #destfile = path + '/' + filename

                #if os.path.isfile(destfile):
                        #os.remove(destfile)
                        #time.sleep(2)
				path = config.get('Client', 'Notify_path')
                while True:			
                        msg = subscriber.recv()
                        if msg:
							msg = msg.split(",")
							if (path == msg[2][:len(path)]):
								if msg[3] in ("01CREAT", "05MKNOD"):
									print(path+' CREATE '+msg[2][len(path):])

								elif msg[3] == "02MKDIR":
                                    print(path+' CREATE,ISDIR '+msg[2][len(path):])

								elif msg[3] == "06UNLNK":
                                    print(path+' DELETE '+msg[2][len(path):])

								elif msg[3] == "07RMDIR":
                                    print(path+' DELETE,ISDIR '+msg[2][len(path):])

								elif msg[3] == "08RENME":
                                    print(path+' MOVED_FROM /'+msg[4]+path+' MOVED_TO '+msg[2][len(path):])

								elif msg[3] == "11CLOSE":
                                    print(path+' CLOSE '+msg[2][len(path):])

								elif msg[3] in ("17MTIME", "18CTIME"):
                                    print(path+' MODIFY '+msg[2][len(path):])

                                #f = open(destfile, 'a+')
                                #print('open')
                                #logging.info(msg)
                                #f.write(msg)
                                #print('close')
                                #f.close()

		def receiveUnix(self):
			global src_path
			src_path = config.get('Client', 'Notify_path')
			observer = Observer()
    		observer.schedule(MyHandler(), path=src_path if src_path else '.',recursive=True)
			observer.start()
			try:
        		while True:
            		time.sleep(1)
    		except KeyboardInterrupt:
        		observer.stop()

    		observer.join()

class MyHandler(PatternMatchingEventHandler):

    	def process(self, event):
        	"""
        	event.event_type 
            		'modified' | 'created' | 'moved' | 'deleted'
        	event.is_directory
            		True | False
        	event.src_path
            		path/to/observed/file
        	"""
        	# the file will be processed there
        	#print(event.src_path, event.event_type)  # print now only for degug
		#def on_any_event(self, event):
        #        print(event.event_type+' printed')

    	def on_modified(self, event):
			if(event.src_path[len(src_path):] != ''):
				if(event.is_directory):
					print(src_path+' MODIFY,ISDIR '+event.src_path[len(src_path):])
                    print(src_path+' CLOSE,ISDIR '+event.src_path[len(src_path):])
        		else:
					print(src_path+' MODIFY '+event.src_path[len(src_path):])
					print(src_path+' CLOSE '+event.src_path[len(src_path):])
    	
		def on_created(self, event):
			if(event.is_directory):
        		print(src_path+' CREATE,ISDIR '+event.src_path[len(src_path):])
			else:
				print(src_path+' CREATE '+event.src_path[len(src_path):])

		def on_deleted(self, event):
			if(event.is_directory):
                print(src_path+' DELETE,ISDIR '+event.src_path[len(src_path):])
			else:
				print(src_path+' DELETE '+event.src_path[len(src_path):])

		def on_moved(self, event):
            print(src_path+' MOVED_FROM '+event.src_path[len(src_path):])
			print(src_path+' MOVED_TO '+event.dest_path[len(src_path):])

if __name__ == '__main__':
	global config
    config = ConfigParser.ConfigParser()
    config.read('config_unix_client.ini')
	logging.basicConfig(filename='log_Worker00.log', level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filemode ='w', maxBytes=5*1024*1024)
    #print = logging.info
    subscribeChangelog().monitor()