#!/usr/bin/python

__author__="David Toporowski"
__date__="$Mar 5, 2013 09:52:56 AM$"

"""
This script connects to an Apple API and retreives the fields CountryCode, AppName, Downloads, Updates and Datecreated
and inserts them in the DW_Daily_Apple_Metrics table.  The stats are to analyze our products or apps by country.
"""

import os
import sys
import urllib2
import socket
import json
from db import _conn
import time
import datetime

def getInsertAppleData(statDate):

	statDate = str(statDate)

	uName = "dtoporowski@kobo.com"
	pWord = "4Ac2hewk"
	userData = "Basic " + (uName + ":" + pWord).encode("base64").rstrip()

	statUrl = "https://api.appfigures.com/v1.1/sales/countries+products/%s/" % statDate + "%s" % statDate
	req = urllib2.Request(statUrl)
	req.add_header('GET', '/v1.1/users/dtoporowski@kobo.com HTTP/1.1')
	req.add_header('Host', 'api.appfigures.com')
	req.add_header('Content-Type', 'application/json')
	req.add_header('Authorization', userData)

	# make the request and get the data
	socket.setdefaulttimeout(120)
	res = urllib2.urlopen(req) # <type 'instance'>
	output = res.read() # <type 'str'>
	jdata = json.loads(output) # dict type

	conn = _conn()
	cur = conn.cursor()
	cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

	for countryCode in jdata:

		for device in jdata[countryCode]: # device is really related to appName (also know as sku inside the apple data)

			if int(device) == 32119:
				appName = "Kobo for IOS"
			else: # 57610
				appName = "Kobo for IOS International"

          		insertQuery = "INSERT INTO ShortCover_DW.dbo.DW_Daily_Apple_Metrics (CountryCode, AppName, Downloads, Updates, Datecreated)" \
          		    	    + " VALUES('%s', " % countryCode + "'%s', " % appName \
          		    	    + "'%s', " % str(jdata[countryCode][device]['downloads']) + "'%s'" % str(jdata[countryCode][device]['updates']) + ", '%s'" % statDate + ")"

			cur.execute(insertQuery)

	conn.commit()
	conn.close()

# actual code for daily run
processDate = datetime.date.today() + datetime.timedelta(-1)

getInsertAppleData(processDate)
sys.exit(0)