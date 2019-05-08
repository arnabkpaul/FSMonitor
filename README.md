Author: Arnab K. Paul

# FSMonitor

Requirements: Python 2.7, ZeroMQ for python (pip install pyzmq), MySQL

Lustre Client/Local File System: 
	
	On Lustre, this module needs to go in the Lustre clients. Edit config_client.ini. Have Type as either 'Lustre' or 'Linux' or 'MAC'. Set the path to monitor in the Notify_Path.
	$ python subscribeChangelog.py

	If client had failed and needs past events, set the 'config_faulttolerance_client.ini' file. 
	$ python faulttolerance_client.py

	An example config file for Linux is given in 'config_unix_client.ini'.

Lustre MGS:
	
	Have this module in the Management Server of the Lustre cluster.
	Edit 'config_mgs.ini'. Follow the rules given in the configuration file.
	Note: Have different port numbers for MGS and MDS.
	Give your MySQL database details in the config file as well.
	$ python subscribeChangelog.py

	To get past events in case of a failure of a client, edit 'config_faulttolerance_mgs.ini'.
	$ python faulttolerance_mgs.ini

Lustre MDS: 

	This module should be loaded in all the Metadata Servers in the Lustre installation. 

	Edit 'config_mds.ini' in all the MDSs. 

	$ python publishChangelog_Cache.py

NOTE:
	
	1. If for some reason, you have to stop the FSMonitor modules for Lustre monitoring. Kill all python processes on MGS and Client.
	$ killall -9 python
	This will allow you to use the same ZMQ ports as the previous runs.

	2. If ZMQ is not allowing network traffic between nodes, try disabling firewall.	
