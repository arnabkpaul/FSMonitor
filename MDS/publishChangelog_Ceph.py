from __future__ import print_function

import collections
import sys, os
import time
from datetime import datetime
from datetime import date
import threading
import subprocess
import uuid
import re
import hashlib
import json
import zmq
import signal
import logging
import ConfigParser

class publishChangelog(object):

    def monitor(self):
        self.ceph_path = config.get('MDS', 'Ceph_Path')
	self.events = []
        self.observe_changelogs()

    def observe_changelogs(self):
        try:
                global context
                context = zmq.Context()
                global publisher
                publisher = context.socket(zmq.PUB)
		portno = config.get('MDS', 'ZMQ_Publisher_Port')
                #publisher.bind("tcp://*:5557")
		publisher.bind("tcp://*:%s"%portno)
                time.sleep(5)
		global fsname
		fsname = config.get('MDS', 'FS_Name')
		rank = config.get('MDS', 'Rank')
		#print('Starting')
                while True:
                        # cephfs-journal-tool --rank=mycephfs:0 event get list
			#arg2 = '--rank='+fsname+':'+rank
                        events = subprocess.check_output(["cephfs-journal-tool", "event", "get", "list"])
			#print(events)
			event = ""
                        for line in events.split('\n'):
				if line.startswith('0x'):
					if event != "":
						if "NOOP" not in event:
							self.on_any_event(event) 
							#print ('new'+event)
						event = ""
				event = event + line
			#subprocess.check_output(["cephfs-journal-tool", "event", "splice", "list"])

        except (KeyboardInterrupt, SystemExit):
                publisher.close()
                context.term()
                sys.exit(0)

    def on_any_event(self, event):
        try:
                """
                        Try to match a rule to this event. If nothing is found, return None
                """
                #logger.debug("Checking rules")
                event = " ".join(event.split())
		event_data = event.split(" ")
		event_id = event_data[0]
		event_type = event_data[1]
		if event_data[2][-1] != ')':
			event_subtype = event_data[2] + '_' + event_data[3]
			file_names = ' '.join(event_data[4:])
		else:
			event_subtype = event_data[2]
			file_names = ' '.join(event_data[3:])
		timestamp = datetime.now().time()
		datestamp = date.today()
		if 'stray' in event_subtype:
			return None
		files = file_names.split(" ")
		for f in files:
			if "stray" in f:
				continue
			etype = event_type + event_subtype
			f = "/mnt/" + fsname + "/" + f
			fsplit = f.rsplit('/', 1)
			path = fsplit[0]
			filename = fsplit[1]
			message =  "%s,%s,%s,%s,%s" % (timestamp, datestamp, path, etype, filename)
			print(message)
                	publisher.send(message)
			#call = "cephfs-journal-tool event splice --range=.."+event_id+" list"
			#subprocess.call(call, shell = True)
                return None
	
	except (IndexError):
		return None


        except (KeyboardInterrupt, SystemExit):
                publisher.close()
                context.term()
                sys.exit(0)

    def signal_handler(self, signum, frame):
        print("Exiting....!!!!!")
        publisher.close()
        context.term()
        sys.exit(0)

if __name__ == "__main__":
	global config
	config = ConfigParser.ConfigParser()	
	config.read('config_mds_ceph.ini')
        signal.signal(signal.SIGINT, publishChangelog().signal_handler)
        signal.signal(signal.SIGTSTP, publishChangelog().signal_handler)
        signal.signal(signal.SIGQUIT, publishChangelog().signal_handler)
        signal.signal(signal.SIGTERM, publishChangelog().signal_handler)
        logging.basicConfig(filename='log_MDS.log', level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filemode ='w', maxBytes=5*1024*1024)
        #print = logging.info
	publishChangelog().monitor()
