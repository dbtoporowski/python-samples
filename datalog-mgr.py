#! /usr/bin/python

__author__="DavidT"
__date__ ="$Mar 25, 2009 9:01:28 AM$"

# The datalog manager script is run by datalog_push.py and given the cmd line argument to
# perform one of the following tasks: flush, rotate, shutdown, status, it sends the task back
# to the daemon to be processed


import sys
from socket import *

# parse args
if len(sys.argv) < 3:
	print 'usage: ' + sys.argv[0] + ' ' \
		+ '<command>' + ' ' + '[<host>]' + ' ' + '<port>'
	sys.exit(1)

cmd = sys.argv[1]
if len(sys.argv) > 3:
	host = sys.argv[2]
	port = int(sys.argv[3])
else:
	host = 'localhost'
	port = int(sys.argv[2])

if cmd != 'flush' and cmd != 'rotate' and cmd != 'shutdown' and cmd != 'status' :
	print 'commands: flush, rotate, shutdown, status'
	sys.exit(1)

sock = socket(AF_INET,SOCK_DGRAM)

exitStatus = 0

try:
	sock.sendto(cmd, (host, port))
	sock.settimeout(2)
	result, addr = sock.recvfrom(100)
	print 'result from ' + host + ':' + str(port) + '->' + result
except SystemExit:
	pass
except IOError:
	for e  in sys.exc_info():
		print "I/O Error: " + str(e)
	exitStatus = 2
except Exception:
	for e  in sys.exc_info():
		print "Exception - " + str(e)
	exitStatus = 2
except Error:
	for e  in sys.exc_info():
		print "Error - " + str(e)
	exitStatus = 2

sock.close()
sys.exit(exitStatus)
