#!/usr/bin/python

__author__="David Toporowski"
__date__="$Jan 14, 2014 09:37:34 AM$"

"""
This script exports the excel spreadsheet from the Google docs spreadsheet service
that the Sales team uploads and parses the data (RetailerName, Weekdate, PlatformName, SalesAmount)
into the DW_Weekly_Retailer_Sales table.  It is in crontab and runs weekly
"""

import gdata.docs.service
import gdata.spreadsheet.service
import os
import sys
from datetime import date, datetime, timedelta
import xlrd
from stats import db

class Ddict(dict):
    def __init__(self, default=None):
        self.default = default
    def __getitem__(self, key):
        if not self.has_key(key):
            self[key] = self.default()
        return dict.__getitem__(self, key)

def get_spreadsheet(key, file_path, gid=0):

  gd_client = gdata.docs.service.DocsService()
  gd_client.email = 'NotRealEmail@NotReal.com'
  gd_client.password = 'NotRealPassword'
  gd_client.ssl = True
  gd_client.source = "Weekly Report Downloader"
  gd_client.ProgrammaticLogin()

  # also set spreadsheet client login as otherwise get a 404 unautorized error
  spreadsheets_client = gdata.spreadsheet.service.SpreadsheetsService()
  spreadsheets_client.email = gd_client.email
  spreadsheets_client.password = gd_client.password
  spreadsheets_client.source = gd_client.source
  spreadsheets_client.ProgrammaticLogin()

  uri = 'http://docs.google.com/feeds/documents/private/full/%s' % key

  entry = gd_client.GetDocumentListEntry(uri)
  docs_auth_token = gd_client.GetClientLoginToken()
  gd_client.SetClientLoginToken(spreadsheets_client.GetClientLoginToken())
  gd_client.Export(entry, file_path)
  gd_client.SetClientLoginToken(docs_auth_token) # reset the DocList auth token


def xldate_as_datetime(xldate, datemode):
    if datemode not in (0, 1):
        raise XLDateBadDatemode(datemode)
    if xldate == 0.00:
        return datetime.time(0, 0, 0)
    if xldate < 0.00:
        raise XLDateNegative(xldate)
    xldays = int(xldate)
    frac = xldate - xldays
    seconds = int(round(frac * 86400.0))
    assert 0 <= seconds <= 86400
    if seconds == 86400:
        seconds = 0
        xldays += 1

    if xldays == 0:
        # second = seconds % 60; minutes = seconds // 60
        minutes, second = divmod(seconds, 60)
        # minute = minutes % 60; hour    = minutes // 60
        hour, minute = divmod(minutes, 60)
        return datetime.time(hour, minute, second)

    if xldays < 61 and datemode == 0:
        raise XLDateAmbiguous(xldate)

    return (
        datetime.fromordinal(xldays + 693594 + 1462 * datemode)
        + timedelta(seconds=seconds)
        )

def parse_spreadsheet_into_db(file_path):

	cur.execute("TRUNCATE TABLE ShortCover_DW.dbo.DW_Weekly_Retailer_Sales")

	retailerAliasesDict = Ddict( dict )
	retailerAliaseQuery = "SELECT AliaseName, RetailerName FROM ShortCover_Stats.dbo.Retailer_Aliases"
	cur.execute(retailerAliaseQuery)
	rs = cur.fetchall()
	for row in rs:
        retailerAliasesDict[row[0]] = row[1] # set to retailer name to use

	wb = xlrd.open_workbook(file_path)

	sheets = wb.sheet_names()

	for sheetname in sheets: # sheet is the platform/device

		sheet = wb.sheet_by_name(sheetname)

		platform = sheetname

		print "platform is %s " % platform

        if platform == 'K3S' or platform == 'Combined2' or platform == 'Combined (not accurate)':
            continue

		retailers = []
		retailers = sheet.row_values(0)

		if platform == "Active Kobo1" or platform == "Kobo1":
			platform = "Kobo Reader"
			locationrow = 4 # for AUNZ or Canada Refurbs
			rownum = 5 # 1st date in file after headers
		elif platform == "Active Kobo 2" or platform == "Kobo 2":
		    platform = "Kobo WiFi"
		    locationrow = 4
		    rownum = 5 # 1st date in file after headers
		elif platform == "Active Kobo 3" or platform == "Kobo 3":
		    platform = "Kobo Touch"
		    locationrow = 4
			rownum = 5
		elif platform == "Active Kobo 4" or platform == "Kobo 4 (vox)":
			platform = "Kobo Vox"
			locationrow = 1
			rownum = 31
		else:
			print "this device is not currently supported in this process"
			# send myself and jordan a notification email to add it in
			continue

		while rownum < sheet.nrows:

			# dont need totals and if date is blank skip
		    if sheet.cell(rownum, 0).value == "Total" or sheet.cell(rownum, 0).value == "":
		    	break

			weekdate = xldate_as_datetime(sheet.cell(rownum, 0).value, 0)

			colnum = 1
			while colnum < sheet.ncols:

				if retailers[colnum] == "Total":
					break

				value = str(sheet.cell(rownum, colnum).value)
                if len(value) > 0 and value[0] == "=":
                	value = 0

                if value == '' or value == None:
                    value = 0

				retailer = retailers[colnum]

				if retailer == "Refubs" or retailer == "Refurbs":
					retailer = "Refurbs " + str(sheet.cell(locationrow, colnum).value)

				# if retailer has an alterntive ret name entered in the file get from dict object of aliases
				if retailer in retailerAliasesDict:
					retailer = retailerAliasesDict[retailer]

				insertQuery = "insert into ShortCover_DW.dbo.DW_Weekly_Retailer_Sales (RetailerName, Weekdate, PlatformName, SalesAmount)" \
					    + " VALUES('%s'," % retailer + "'%s'," % weekdate + "'%s'," % platform + "%s" % value + ")"

				cur.execute(insertQuery)

				colnum += 1

            rownum += 1

conn = db._conn()
cur = conn.cursor()

key = "0BXx3b32jHqM0dGpRVnRAl3NRbkpVWkkdT3nZjBYTUcR"

file_path = "/home/dtops/reporting/Weekly Sales.xls"

# excel file online missing first bunch of dates for Active Kobo 4 device so script will not insert that device until "dummy dates" added to the file
get_spreadsheet(key, file_path)
parse_spreadsheet_into_db(file_path)

print "commiting to db"
conn.commit()
conn.close()

print "script done"
sys.exit(0)
