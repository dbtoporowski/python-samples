#! /usr/bin/python

__author__="David T"
__date__ ="$Apr 1, 2009 10:53:35 AM$"

# Runs in crontab every 5 minutes  */5 * * * * datalog /opt/UDPLogger/datalog_push.py -a > /dev/null
# Rotates the log file and then loads the data into the database

from ConfigParser import ConfigParser
import os
import sys
import subprocess
import fnmatch

def loadConfig(iniFile):
	inFile = open(iniFile, "r")
	config = ConfigParser()
	config.read(iniFile)
	return config

def rotateLogger(port):
	script_dir = os.path.dirname(__file__)
	return subprocess.call([script_dir + '/datalog-mgr.py', 'rotate', str(port)])

def pushLogs(logroot, port, db_host, dbu_user, db_pass, db_name, db_table, db_escape, db_columns):
	procdir = logroot + '/process'
	archdir = logroot + '/archive'
	file_list = os.listdir(procdir)
	log_pattern = 'datalog-'+ str(port) + '*'
	if db_escape == '\\' :
		db_escape = '\\\\'
	mysql = ['mysql', '-h', db_host, '-u', db_user, '-p' + db_pass, '-D', db_name, '-e' ]
	loadData1 = "LOAD DATA LOCAL INFILE '"
	loadData2 = "' INTO TABLE " + db_table + r" COLUMNS TERMINATED BY '\t' ESCAPED BY '" + db_escape + "' " + db_columns
	# load the file data into the db, then archive the file renaming it to the archive directory
	for file in file_list :
		if fnmatch.fnmatch(file, log_pattern) :
			load = loadData1 + procdir + '/' + file + loadData2
			result = subprocess.call(['mysql', '-h', db_host, '-u', db_user, '-p' + db_pass, '-D', db_name, '-e', load ])
			if result != 0 :
				return result
			subprocess.call(['mv', procdir + '/' + file, archdir + '/' + file])
	return 0

logroot = '/var/log/datalog/'

service_name = ''
iniFile = os.path.dirname(__file__) + '/datalog.ini'

if len(sys.argv) > 1 :
	service_name = sys.argv[1]

if len(sys.argv) > 2 :
	iniFile = sys.argv[2] # scriptname -a ini_file

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
	db_host = ''
	db_user = ''
	db_pass = ''
	db_name = ''
	db_table = ''
	db_escape = ''
	db_columns = ''
	archive_days = 14
	section_error = False
	option_list = config.options(section)
	for option in option_list :
		if option == 'port' :
			port = int(config.get(section, option))
		elif option == 'db_host' :
			db_host = config.get(section, option)
		elif option == 'db_user' :
			db_user = config.get(section, option)
		elif option == 'db_pass' :
			db_pass = config.get(section, option)
		elif option == 'db_name' :
			db_name = config.get(section, option)
		elif option == 'db_table' :
			db_table = config.get(section, option)
		elif option == 'db_escape' :
			db_escape = config.get(section, option)
		elif option == 'db_columns' :
			db_columns = config.get(section, option)
		elif option == 'archive_days' :
			archive_days = int(config.get(section, option))

	if port < 1023 or port > 32768 :
		print 'Section ' + section + ' port number is missing or bad'
		section_error = True
	if len(db_host) == 0 or len(db_name) == 0 or len(db_table) == 0 or len(db_user) == 0 or len(db_pass) == 0:
		if len(db_host) > 0 :
			print 'Section ' + section + 'has no database host for pushing'
		else :
			print 'Section ' + section + ' database definition is incomplete'
		section_error = True

	if service_name == section or service_name == '-a' :
		noSection = False
		if section_error :
			print "Can't push logs for " + section
		else :
			print "Pushing logs for " + section
			# rotate logs
			if rotateLogger(port) != 0 :
				print "Log rotation failed for " + section
			# dumnp logs data into a db table
			elif pushLogs(logroot, port, db_host, db_user, db_pass, db_name, db_table, db_escape, db_columns) != 0 :
				print "Push failed for " + section
			else :
				print "Push suceeded for " + section

	subprocess.call(['find','/var/log/datalog/archive', '-type', 'f', '-mtime', '+' + str(archive_days), \
                '-name','"datalog-' + str(port) +'*"', '-exec', 'rm', '{}', ';'])

if noSection :
	print "No such service: " + service_name
