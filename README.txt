Universal Stats - UDP Data Listener Daemon

Files:
UDPLogger.php	(located within the webkinz API)

The following files reside on a seperate server:
config.ini
datalog_start.py
datalog-s.py	(main script - data listening daemon)
datalog_push.py (run every 5 minutes to rotate the logfile and do bulk database inserts of the logfile data)
datalog-mgr.py
datalog_stop.py

Description
The daemon (datalog-s.py) listens for UDP data packets received from the UDPLogger.php class
which is located within the Webkinz API.  It was first created to capture page view stats of 
adobe flash files by view_id, but is now receiving game metrics and others stats data as well.






