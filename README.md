Author: Arnab K. Paul

# FSMonitor

Requirements: Python 2.7, ZeroMQ for python (pip install pyzmq), MySQL, Watchdog for local file system (pip install watchdog)

Lustre Client/Ceph Client/Local File System: 
	
	On Lustre and Ceph, this module needs to go in the Lustre clients. 
	
	Edit config_client.ini. Have Type as either 'Lustre' or 'Ceph' or 'Linux' or 'MAC'. Set the path to monitor in the Notify_Path.
	$ python subscribeChangelog.py

	If client had failed and needs past events, set the 'config_faulttolerance_client.ini' file. 
	$ python faulttolerance_client.py

	An example config file for Linux is given in 'config_unix_client.ini'.

	For fault tolerance in Ceph, config file is in 'config_faulttolerance_ceph_client.ini'.
	$ python faulttolerance_ceph_client.py

Aggregator:
	
	Have this module in the Management Server of the Lustre cluster or Monitor in the Ceph Object Store.
	
	For Lustre: 
		Edit 'config_mgs.ini'. Follow the rules given in the configuration file.
		Note: Have different port numbers for MGS and MDS.
		Give your MySQL database details in the config file as well.
		$ python subscribeChangelog.py

		To get past events in case of a failure of a client, edit 'config_faulttolerance_mgs.ini'.
		$ python faulttolerance_mgs.py

	For Ceph:
                Edit 'config_mon_ceph.ini'. Follow the rules given in the configuration file.
                Note: Have different port numbers for Monitor and MDS.
                Give your MySQL database details in the config file as well.
                $ python subscribeChangelog_ceph.py

                To get past events in case of a failure of a client, edit 'config_faulttolerance_mgs.ini'.
                $ python faulttolerance_mon_ceph.py

MDS: 

	This module should be loaded in all the Metadata Servers in the Lustre and Ceph installation. 
	
	For Lustre:
		Edit 'config_mds.ini' in all the MDSs. 

		$ python publishChangelog_Cache.py
	
	For Ceph:
                Edit 'config_mds_ceph.ini' in all the MDSs.

                $ python publishChangelog_Ceph.py

NOTE:
	
	1. If for some reason, you have to stop the FSMonitor modules for Lustre monitoring. 
		Kill all python processes on MGS and Client.
		$ killall -9 python
		This will allow you to use the same ZMQ ports as the previous runs.

	2. If ZMQ is not allowing network traffic between nodes, try disabling firewall.
		$ sudo systemctl stop firewalld

Order of running the modules:
	For Event Collection on Local File Systems:
		Client -> $ python subscribeChangelog.py

	For Event Collection in Lustre:
		Client -> $ python subscribeChangelog.py
		MGS -> $ sudo python subscribeChangelog.py
		All MDSs -> $ python publishChangelog_Cache.py

	For Event Collection in Ceph:
                Client -> $ python subscribeChangelog.py
                Monitor -> $ sudo python subscribeChangelog_ceph.py
                All MDSs -> $ python publishChangelog_Ceph.py

	For Fault Tolerance in Lustre:
		MGS -> $ python faulttolerance_mgs.py
		Client -> $ python faulttolerance_client.py

	For Fault Tolerance in Ceph:
                Monitor -> $ python faulttolerance_mon_ceph.py
                Client -> $ python faulttolerance_ceph_client.py
