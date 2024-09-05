#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="David Toporowski"
__date__="$Jan 14, 2015 12:45:11 PM$"

"""
This script is called by the run.get.crm.bi.discoverer.db.py cron script to send all the BI tables contained in the
CRM_Database_Details_Tables table with the status field set to 1 to the 3rd party marketing and analytics company
Race Data. It sftps one table file at a time.  Our marketing department updates this framework table accordingly as
to which tables they want send over.  It gets the field names from the CRM_Database_Details_Fields framework table.
"""

import os
import sys
import csv
import datetime
from datetime import date, timedelta
from uuid import *
import paramiko
from paramiko import SFTPClient
import pymssql
from unidecode import unidecode
import cStringIO,codecs
import gzip

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)



def sendFile(csvfilename):

    # racedata sftp
    host = "NotRealHost.com"
    username = "NotRealUserName"
    password = "NotRealPassword"
    path = "Inbound/%s" % str(csvfilename)

    port = 22
    transport = paramiko.Transport((host, port))
    transport.connect(username = username, password = password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    localpath = "/home/iislogs/reports/%s" % csvfilename
    sftp.put(localpath, path)
    sftp.close()
    transport.close()

    #archive the csv file
    archivePath = "/home/iislogs/logs/archive/racedata"
    mvcmd = "mv %s" % csvfilename + " %s" % archivePath
    os.popen(mvcmd)

    return

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s', filename="/home/iislogs/reports/crm-debug.log")

info("script start")

conn = pymssql.connect(host='10.111.12.1', user='ReportUser', password='rpt@K0b0', database='ShortCover_Report', as_dict=False, charset='utf8')
cur = conn.cursor()

date_str = date.today().strftime('%Y%m%d')

tableid = sys.argv[1]
tablename = sys.argv[2]
csvfilenameprefix = tablename
csvfilename = csvfilenameprefix + "_" + str(date_str) + ".csv.gz"

queryFields = """SELECT FieldName, FieldType, CsvHeaderTitle, CsvFieldSequence
				 FROM ShortCover_DW.dbo.CRM_Database_Details_Fields f
  				 WHERE f.tableid = %d ORDER BY CsvFieldSequence""" % tableid

cur.execute(queryFields)
rsFields = cur.fetchall()
tableFields = []
csvHeaderTitles = []

# loop through each field beloinging to the table id parameter passed to this script
for rowFields in rsFields:
	tableFields.append(rowFields[0])
	csvHeaderTitles.append(rowFields[2])

tableFieldsStr = ', '.join( tableFields )
queryData = """SELECT %s FROM ShortCover_DW.dbo.%s""" % (tableFieldsStr, tablename)
info(queryData)
cur.execute(queryData)

csvfilehandle = gzip.open("/home/iislogs/reports/%s" % csvfilename, 'wb')
fileHandle = UnicodeWriter(csvfilehandle, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
fileHandle.writerow(csvHeaderTitles)

for rowdata in cur:
	fileHandle.writerow(map(unicode, rowdata))

csvfilehandle.close()

# zip the file, sftp it and archive it
sendFile(csvfilename)

info("race data script done, csv file sftpd and archived")
