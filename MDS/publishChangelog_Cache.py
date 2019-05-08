from __future__ import print_function

import collections
import sys, os
import time
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

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
	#print('capacity', capacity)
        self.cache = collections.OrderedDict()

    def get(self, key):
        try:
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        except KeyError:
            return -1

    def set(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value

class publishChangelog(object):

    def monitor(self):
        #self.lustre_path = "/mnt/scratch/"
        self.lustre_path = config.get('MDS', 'Lustre_Path')
	self.events = []
        self.users = []
        self.client = []
        # get the tasks
        self.last_eid = '0'
        self.observe_changelogs()
        #t3 = threading.Thread(target=self.observe_changelogs)
        #t3.setDaemon(True)
        #t3.start()
        #t3.join()

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
		global mdsname
		mdsname = config.get('MDS', 'MDS_Name')
		#print('Starting')
                while True:
                        # sudo lfs changelog scratch-MDT0000
                        events = subprocess.check_output(["sudo", "lfs", "changelog", mdsname])
                        #if events:
                                #logging.info("Printing events")
                                #logging.info(events)
                        # do the reverse look up on each event
                        #event_list = []
                        for event in events.split('\n'):
                                event_data = event.split(" ")
                                if len(event_data) < 2:
                                        continue
                                fid = ''
                                self.on_any_event(event)

                        #print("Event List")
                        #print event_list
                        #time.sleep(1)
                        #print("looping")
                        # sys.exit(0)

        except (KeyboardInterrupt, SystemExit):
                publisher.close()
                context.term()
                sys.exit(0)

    def on_any_event(self, event):
        try:
                """
                        Raise an event to be processed.
                """
                self.check_rules(event)
		changelog = "mdd."+mdsname+".changelog_users"
                users = subprocess.check_output(["sudo", "lctl", "get_param", changelog])
                #print users
                for user in users.split('\n'):
                        user_data = user.split(" ")
                        if "cl" in user_data[0]:
                                #print ("Client %s" %(user_data[0]))
                                subprocess.check_output(["sudo", "lfs", "changelog_clear", mdsname, user_data[0], self.last])

        except (KeyboardInterrupt, SystemExit):
                publisher.close()
                context.term()
                sys.exit(0)

    def check_rules(self, event):
        try:
                """
                        Try to match a rule to this event. If nothing is found, return None
                """
                #logger.debug("Checking rules")
                event_data = event.split(" ")
                fid = ''
                eid = event_data[0]
                self.last = eid
                event_type = event_data[1]
                timestamp = event_data[2]
                datestamp = event_data[3]
                src_path = ''
                for e in event_data:
                        if 't=[' in e:
                                fid = e[e.find("[")+1:e.find("]")]
                                if fid not in self.events:
					src_path = lru.get(fid)
					#print('found')
					if src_path == -1:
						#print('not found')
                                                # lfs fid2path /mnt/scratch 0x200000401:0x5:0x0
                                                src_path = subprocess.check_output(["sudo", "lfs", "fid2path", self.lustre_path, fid])
						lru.set(fid, src_path)
                # Extract the file and path names
                # src_path = os.path.abspath(event.src_path)
                event_path = src_path.rsplit(os.sep, 1)[0]
                file_name = src_path.rsplit(os.sep, 1)[1]
                src_path = src_path[:len(src_path)-1]
                if 'glink' in file_name:
                        return
                message =  "%s,%s,%s,%s,%s" % (timestamp, datestamp, src_path, event_type, file_name)
                #logging.info(message)
		#print("message", message)
                publisher.send(message)
                return None

        except (KeyboardInterrupt, SystemExit):
                publisher.close()
                context.term()
                sys.exit(0)


        except subprocess.CalledProcessError:
                try:
                        #logging.exception("fid2path error")
                        if any(typ in event_type for typ in ('UNLNK', 'RMDIR')):
                                for e in event_data:
                                        if 'p=[' in e:
                                                fid = e[e.find("[")+1:e.find("]")]
                                                if fid not in self.events:
							src_path = lru.get(fid)
							#print('error')
							if src_path == -1:
                                                        	# lfs fid2path /mnt/scratch 0x200000401:0x5:0x0
                                                        	src_path = subprocess.check_output(["sudo", "lfs", "fid2path", self.lustre_path, fid])
								lru.set(fid, src_path)
                                file_name = event_data[7]
                                src_path = src_path[:len(src_path)-2]+file_name
                                message =  "%s,%s,%s,%s,%s\n" % (timestamp, datestamp, src_path, event_type, file_name)
                                #logging.info(message)
				#print(message)
                                if 'glink' in file_name:
                                        return
                                publisher.send(message)

                        if 'RENME' in event_type:
                                for e in event_data:
                                        if 's=[' in e:
                                                fid = e[e.find("[")+1:e.find("]")]
                                                if fid not in self.events:
							#new_src_path = lru.get(fid)
							new_src_path = -1
                                                        #print('rename error')
                                                        if new_src_path == -1:
								#print('new src path not found')
                                                        	# lfs fid2path /mnt/scratch 0x200000401:0x5:0x0
                                                        	new_src_path = subprocess.check_output(["sudo", "lfs", "fid2path", self.lustre_path, fid])
								lru.set(fid, new_src_path)
                                        if 'sp=[' in e:
                                                old_fid = e[e.find("[")+1:e.find("]")]
                                                if old_fid not in self.events:
							#old_src_path = lru.get(old_fid)
                                                        #print('rename error', old_src_path)
							old_src_path = -1
                                                        if old_src_path == -1:
								#print('old src path not found')
                                                        	# lfs fid2path /mnt/scratch 0x200000401:0x5:0x0
                                                        	old_src_path = subprocess.check_output(["sudo", "lfs", "fid2path", self.lustre_path, old_fid])
								lru.set(old_fid, old_src_path)
                                new_file_name = event_data[7]
                                old_file_name = event_data[10]
                                old_src_path = old_src_path[:len(old_src_path)-2]+old_file_name
                                new_src_path = new_src_path[:len(new_src_path)-1]
                                message =  "%s,%s,%s,%s,%s\n" % (timestamp, datestamp, new_src_path, event_type, old_file_name)
                                #logging.info(message)
				#print(old_src_path)
				#print(new_file_name)
				#print(message)
                                if 'glink' in new_file_name:
                                        return
                                publisher.send(message)

                except subprocess.CalledProcessError:
                        #logging.exception("fid2path error within error")
                        if any(typ in event_type for typ in ('UNLNK', 'RMDIR')):
                                file_name = event_data[7]
                                src_path = 'ParentDirectoryRemoved'
                                message =  "%s,%s,%s,%s,%s\n" % (timestamp, datestamp, src_path, event_type, file_name)
                                #logging.info(message)
				#print(message)
                                if 'glink' in file_name:
                                        return
                                publisher.send(message)

    def signal_handler(self, signum, frame):
        print("Exiting....!!!!!")
        publisher.close()
        context.term()
        sys.exit(0)

if __name__ == "__main__":
	global config
	config = ConfigParser.ConfigParser()	
	config.read('config_mds.ini')
        global lru 
	lru = LRUCache(config.get('MDS', 'Cache_Capacity'))
        signal.signal(signal.SIGINT, publishChangelog().signal_handler)
        signal.signal(signal.SIGTSTP, publishChangelog().signal_handler)
        signal.signal(signal.SIGQUIT, publishChangelog().signal_handler)
        signal.signal(signal.SIGTERM, publishChangelog().signal_handler)
        logging.basicConfig(filename='log_MDS.log', level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filemode ='w', maxBytes=5*1024*1024)
        #print = logging.info
	publishChangelog().monitor()
