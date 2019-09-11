#! /usr/bin/python

__author__="DavidT"
__date__="$Mar 31, 2009 4:45:08 PM$"

# shuts down the data listening daemon via the datalog-mgr.py which lets the datalog-s.py script know
# to shutdown

from ConfigParser import ConfigParser
import os
import sys
import subprocess


def loadConfig(iniFile):
	inFile = open(iniFile, "r")
	config = ConfigParser()
	config.read(iniFile)
	return config

def stopLogger(port):
	script_dir = os.path.dirname(__file__)
	subprocess.call([script_dir + '/datalog-mgr.py', 'shutdown', str(port)])

service_name = ''
iniFile = os.path.dirname(__file__) + '/datalog.ini'
if len(sys.argv) > 1 :
	service_name = sys.argv[1]

if len(sys.argv) > 2 :
	iniFile = sys.argv[2]

if len(service_name) == 0 :
	print 'usage: ' + sys.argv[0] + ' <service_name> [<ini_file>]'
	print '   or: ' + sys.argv[0] + ' -a [<ini_file>]'
	sys.exit(1)

config = loadConfig(iniFile)
section_list = config.sections()
if len(section_list) == 0 :
	print 'No services appear in ' + iniFile
	sys.exit(1)

noSection = True

for section in section_list :
	port = 0
	section_error = False
	option_list = config.options(section)
	for option in option_list :
		if option == 'port' :
			port = int(config.get(section, option))
	if port < 1023 or port > 32768 :
		print 'Section ' + section + ' port number is missing or bad'
		section_error = True
	if service_name == section or service_name == '-a' :
		noSection = False
		if section_error :
			print "Can't stop " + section
		else :
			print "Stopping " + section
			stopLogger(port)

if noSection :
	print "No such service: " + service_name
