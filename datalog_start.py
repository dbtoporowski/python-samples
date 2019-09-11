#! /usr/bin/python

__author__="DavidT"
__date__="$Mar 31, 2009 3:30:18 PM$"

# Running this script from the cmd line is the preferred way to invoke the daemon script
# usage: ./datalog_start.py <service_name> [<ini_file>]
# or ./datalog_start.py -a [<ini_file>]

from ConfigParser import ConfigParser
import os
import sys
import subprocess


def loadConfig(iniFile):
	inFile = open(iniFile, "r")
	config = ConfigParser()
	config.read(iniFile)
	return config

def startLogger(port, delimiter, timestamp):
	script_dir = os.path.dirname(__file__)
	arg_list = [script_dir + '/datalog-s.py']
	if timestamp == False :
		arg_list.append('-n')
	arg_list.append(str(port))
	if delimiter != '\t' :
		arg_list.append(delimiter)
	subprocess.call(arg_list)


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
	delimiter = "\t"
	timestamp = False
	section_error = False
	option_list = config.options(section)
	for option in option_list :
		if option == 'port' :
			port = int(config.get(section, option))
		elif option == 'delimiter' :
			delimiter = config.get(section, option)
		elif option == 'timestamp' :
			timestamp = config.get(section, option).lower() == 'true'
	if port < 1023 or port > 32768 :
		print 'Section ' + section + ' port number is missing or bad'
		section_error = True
	elif len(delimiter) != 1 :
		print 'Section ' + section + ' delimiter has bad value'
		section_error = True
	if service_name == section or service_name == '-a' :
		noSection = False
		if section_error :
			print "Can't start " + section
		else :
			print "Starting " + section
			startLogger(port, delimiter, timestamp)

if noSection :
	print "No such service: " + service_name
