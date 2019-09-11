#!/usr/bin/python

__author__="DavidT"
__date__="$Mar 20, 2009 10:10:32 AM$"

# UDP Data listening daemon that receives data from a php script in webkinz to capture flash page views and other stats
# Every 5 minutes it is also sent the commands from the datalog_push.py script to flush the buffer and rotate the logfile
# and do bulk inserts to the stats database.  It also handles shutdown and status commands directly from the cmd line

import sys
import os
import time
import signal
from socket import *
from syslog import *

# ignore hup
signal.signal(signal.SIGHUP,signal.SIG_IGN)

logroot = '/var/log/datalog/'
filebuffer = 65536 # set to max amount of bytes allowed

# parse args
if len(sys.argv) < 2:
    print 'usage: ' + sys.argv[0] + ' [-n] ' \
		+ '<port>' + ' ' + '[<seperator_defaults_to_tab>]'
    print
    print '      -n       eliminate trailing timestamp'
    sys.exit(1)

add_timestamp = True
portarg = 1
if sys.argv[1] == "-n" :
	portarg = 2
	add_timestamp = False

port = int(sys.argv[portarg])

sep = "\t"
if len(sys.argv) > (portarg + 1):
	sep = sys.argv[portarg + 1];

# set service name
servicename ="datalog-daemon-" + str(port)
# set outfile file name
output = logroot + 'active/datalog-' + str(port) + '.log'
# bind host
host = '0.0.0.0'


# functions
def log(msg, lvl = LOG_NOTICE): syslog(lvl, msg)

def daemonize():
	try:
  		pid = os.fork();
  	except OSError, e:
		raise Exception, "%s [%d]" % (e.strerror, e.errno)

	if(pid == 0): # child 1
		os.setsid()
		try:
			pid = os.fork();
		except OSError, e:
			raise Exception, "%s [%d]" % (e.strerror, e.errno)

		if(pid == 0): # child 2, this one lives
			pass
		else:
			os._exit(0)
	else: # clean up intial parent
		os._exit(0)

	os.chdir('/')
	os.umask(0);

	import resource		 # Resource usage information.
 	maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
 	if (maxfd == resource.RLIM_INFINITY):
		maxfd = MAXFD
		for fd in range(0, maxfd):
			try:
				os.close(fd)
			except OSError: # ERROR, fd wasn't open to begin with (ignored)
				pass

	# dump std streams
	os.open('/dev/null', os.O_RDWR)  # standard input (0)
	os.dup2(0, 1)
	os.dup2(0, 2)

def rotate():
	logfile.close()
	now = time.localtime()
	buff = "%4d-%02d-%02d_%02d-%02d-%02d" % (now[0],now[1],now[2],now[3],now[4],now[5])
	newname = logroot + 'process/datalog-' + str(port) + '_' + buff + '.log';
	log("archiving data to " + newname, LOG_NOTICE);
	os.rename(output, newname)

def shutdown():
	logfile.close()
	sock.close()
	sys.exit(0)

def mkrec(d, sepp, addtimestamp):
	a = d.split(sepp);
	buff = ''
	delim = ''
	for i in a:
		buff = buff + delim +  i.strip()
		delim = "\t"

	if addtimestamp :
		now = time.localtime()
		buff = buff + "\t" + "%4d-%02d-%02d %02d:%02d:%02d" % (now[0],now[1],now[2],now[3],now[4],now[5])
	return buff;

# bootstrap syslog
openlog(servicename,0,LOG_DAEMON)

# set pid filename
pidpath = '/var/run/datalog/' + servicename + '.pid'
if os.path.exists(pidpath):
	ff = open(pidpath,'r')
	oldpid = ff.readline().rstrip()
	print "pid file found, process already running as pid " + oldpid
	sys.exit(1)

## write pid to file
pidfile = open(pidpath,'w',filebuffer)
print >> pidfile, os.getpid()
pidfile.close()

daemonize()

log("starting daemon", LOG_NOTICE);
log("opening file for output " + output, LOG_NOTICE);
logfile =  open(output,'a',filebuffer)
logfileDirty = False

log("binding to " + host + ":" + " port " + str(port), LOG_NOTICE);
sock = socket(AF_INET,SOCK_DGRAM)
sock.bind((host,port))
currentTime = time.clock()
logCount = 0
logRate = 0

try:
	log("receving data, using seperator '" + str(sep) + "'", LOG_NOTICE);
	while 1:
		try:
			data, addr = sock.recvfrom(1024)
			if len(data) == 0 :
				socket.sendto('', addr)

			# every 5 minutes cron (datalog_push.py -a) sends commands to flush the buffer and rotate the logfile
			# shutdown and status commands are invoked from the command line manually
			# OR if the data received is a valid record and not a command it is appended to the logfile
			if data.startswith('flush'):  # forces the buffer content into the file without closing it
				log("flushing log file",LOG_NOTICE);
				logfile.flush();
				sock.sendto('logger flushed', addr)
			elif data.startswith('rotate'):
				if logfileDirty :
					log("rotating log file",LOG_NOTICE);
					rotate();
					logfile = open(output,'a',filebuffer);
					logfileDirty = False
					sock.sendto('logger rotated', addr)
				else :
					sock.sendto('logger not rotated (empty file)', addr)
			elif data.startswith('shutdown'):
				log("shutting down",LOG_NOTICE);
				sock.sendto('logger shut down', addr)
				shutdown();
			elif data.startswith('status'):
				sock.sendto('logger status {LogsPerMinute=' + str(logRate) + '}', addr)
			else :
				print >> logfile, mkrec(data, sep, add_timestamp)
				logfileDirty = True
				logCount = logCount + 1
				newTime = time.clock()
				if newTime > currentTime + 60:
					deltaTime = int((newTime - currentTime) / 60)
					logRate = int(logCount / deltaTime)
					logCount = 0

		except ValueError:
			log('ValueError ' + str(sys.exc_info()[1]) + " " + str(data).rstrip(),LOG_WARNING)
		except TypeError:
			log('TypeError ' + str(sys.exc_info()[1]) + " " + str(data).rstrip(),LOG_WARNING)
except SystemExit:
	pass
except IOError:
	for e  in sys.exc_info():
		log("I/O Error: " + str(e),LOG_CRIT)
except Exception:
	for e  in sys.exc_info():
		log("Exception - " + str(e),LOG_CRIT)
except Error:
	for e  in sys.exc_info():
		log("Error - " + str(e),LOG_CRIT)

os.remove(pidpath)

log("daemon stopped")

