#!/usr/bin/python

__author__="David Toporowski"
__date__="$Jan 14, 2015 12:45:11 PM$"

"""
Cron script to send all the BI tables contained in the CRM_Database_Details_Tables table to the 3rd party
marketing and analytics company Race Data. Loops through the table of marketing tables and runs the script
get.crm.bi.discoverer.db.py passing tableid and tablename with each iteration
"""

import sys
import os
from db import _conn
from logging import info, debug
import logging

conn = _conn()
cur = conn.cursor()

# first get the table to loop thru and then their fields
queryTables = """SELECT TableId, TableName, CsvFilenamePrefix FROM ShortCover_DW.dbo.CRM_Database_Details_Tables WHERE status = 1"""
cur.execute(queryTables)
rsTables = cur.fetchall()

for rowTables in rsTables:

	tableid = rowTables[0]
	tablename = rowTables[1]

	info("Processing table %s" % tablename + " start")
	call = "python get.crm.bi.discoverer.db.py %d %s" % (tableid, tablename)
	info("Processing table %s" % tablename + " end")

	os.popen(call)

