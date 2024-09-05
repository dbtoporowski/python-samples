#!/usr/bin/python

__author__="David Toporowski"
__date__="$Dec 5, 2012 14:05:12 PM$"

"""
Gets iphone game tabs data from the Featured Groups, Featured Lists and Featured_Items tables and combines it
with user data to send off to cheetah mail via sftp for marketing emails, runs nightly in crontab
"""

import os
import sys
import time
import datetime
import calendar
from datetime import timedelta
from time import gmtime, strftime # just for logging benchmarks, can remove after
from db import _conn
from base64 import urlsafe_b64encode as b64encode
import csv
from uuid import *
from collections import defaultdict
import Crypto
import paramiko
from paramiko import SFTPClient

now = datetime.datetime.now()
todaysDatetime = str(now.year) + "-" + "%02d" % now.month + "-" + "%02d" % now.day + " 06:00:00.000"
yesterday = now - timedelta(1)
yesterdaysDatetime  = str(yesterday.year) + "-" + "%02d" % yesterday.month + "-" + "%02d" % yesterday.day + " 06:00:00.000"

# for csv files processed in case we have to re sftp the files to cheetah
archivePath = "/home/iislogs/logs/archive/ios/"

conn = _conn()
cur = conn.cursor()

cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

cur.execute("IF OBJECT_ID('Tempdb..#tmp_user_volumes_tabs_fw') IS NOT NULL DROP TABLE #tmp_user_volumes_tabs_fw;")
tabsFrameworkQuery = select volumeid, t.TabID, t.DisplayTitle, t.Name" \
                        + " into Tempdb..#tmp_user_volumes_tabs_fw" \
                        + " from ShortCover_Report.dbo.TabsV2 t (nolock)" \
                        + " inner join ShortCover_Report.dbo.Featured_Groups fg on fg.FeaturedGroupName like t.FeaturedGroupName + '%'" \
                        + " inner join ShortCover_Report.dbo.Featured_Lists fl on fl.featuredgroupid = fg.FeaturedGroupID" \
                        + " inner join ShortCover_Report.dbo.Featured_Items fi on fi.FeaturedListID = fl.FeaturedListID" \
                        + " where TabID in (" \
                        + "'8BB10A12-EBD8-439C-85DB-0512BA09C071'," \
                        + "'662F905D-E59B-46F5-9208-110E4E305969'," \
                        + "'81FF47FB-91E8-43EE-A47A-18E29DFD9D4C'," \
                        + "'32AA7BB7-FD96-4A45-8547-1D998B8A67FE'," \
                        + "'121CE414-617F-4DF9-9DC5-20FB827CFEF6'," \
                        + "'69C69824-C943-4EA5-98DC-278814F20E3C'," \
                        + "'E45E7C87-7A7B-4332-8B96-2B68D617BCBF'," \
                        + "'EF8A2148-CB7B-42F1-864E-388F1E4FB0B0'," \
                        + "'D4CC53B9-21DD-4C86-9ED2-443E546A5114'," \
                        + "'EFF171DB-B2F3-46CE-8C66-4A63FEB7333F'," \
                        + "'4CAFC8DD-EB51-4DD1-822C-5606A291660C'," \
                        + "'005389FE-D83F-4330-882F-5DB3F8E20313'," \
                        + "'3AE04924-B9C7-4BB3-A0CC-60305F16DE5D'," \
                        + "'FCE2B67F-29E6-4296-8AA3-62CBB18E662B'," \
                        + "'E55DD040-DC71-441F-88F7-643388440015'," \
                        + "'5FA48E6C-0C47-4B19-8975-69CCF2F2B807'," \
                        + "'4B6A73F3-ABFE-4A7D-80BE-71D5D377CBA4'," \
                        + "'A6F327FD-7D35-4521-A709-7363B3794F80'," \
                        + "'F7DB527D-A03D-4C3C-BB72-7BE136EBE484'," \
                        + "'004F9DFF-B9DC-4135-AA3D-82C2EEBAA9DC'," \
                        + "'BC948641-5F25-4D20-A28F-8C5E33BC4744'," \
                        + "'1C624840-D21E-4F01-9DDE-9E162750AE15'," \
                        + "'84E6F4CE-F30D-466A-A357-A69D12980D54'," \
                        + "'57A4E234-647B-426A-A857-B491C96A0E60'," \
                        + "'B53844B1-3C8E-4BC6-82E0-D16F78E6F2BA'," \
                        + "'F6875C2E-71CA-46CF-AB1A-D26FB21252A4'," \
                        + "'6A5C136B-C0D2-45BF-BF9C-D568AC8B5CDB'," \
                        + "'54B27F50-F21E-4D19-A262-D57D8DBB431E'," \
                        + "'58677EFA-E8EA-4A9D-A72C-D5851BF30035'," \
                        + "'DBDAA38B-1A87-445F-ACB2-E36E55EDC606'," \
                        + "'B1F7D11E-B09F-4E56-919A-E3A79BE47CDD'," \
                        + "'738B07F9-7468-4337-893C-EDA057CBFE58'," \
                        + "'F6FBD3B3-F87E-43CA-959B-EFF48C3E6F87'," \
                        + "'530B0FED-95CD-4C49-AE19-FD48A80319A3')" \
                        + " order by DisplayTitle;"
cur.execute(tabsFrameworkQuery)
cur.execute("CREATE CLUSTERED INDEX IDX_VolumeID ON TempDb..#tmp_user_volumes_tabs_fw(volumeid)")

cur.execute("Select * from TempDb..#tmp_user_volumes_tabs_fw")

cur.execute("IF OBJECT_ID('Tempdb..#tmp_user_volumes_tabs_ud') IS NOT NULL DROP TABLE #tmp_user_volumes_tabs_ud;")
tabsUserdataQuery = "select uv.UserID, tfw.TabID, tfw.volumeid, uv.LastModified, tfw.DisplayTitle, tfw.Name" \
                        + " into TempDb..#tmp_user_volumes_tabs_ud" \
                        + " from TempDb..#tmp_user_volumes_tabs_fw tfw (nolock)" \
                        + " inner join ShortCover_Report.dbo.User_Volumes uv ON tfw.volumeid = uv.VolumeID;"
cur.execute(tabsUserdataQuery)

cur.execute("IF OBJECT_ID('Tempdb..#tmp_user_volumes_tabs_ud_counts') IS NOT NULL DROP TABLE #tmp_user_volumes_tabs_ud_counts;")
tabsSameHourQuery = "select TabID, UserID, MIN(LastModified) as StartDate, MAX(LastModified) as EndDate, count(1) as TabCount" \
                        + " into TempDb..#tmp_user_volumes_tabs_ud_counts" \
                        + " from TempDb..#tmp_user_volumes_tabs_ud (nolock)" \
                        + " group by TabID, UserID" \
                        + " having count(1) > 3 AND DATEDIFF(hh, MIN(LastModified), MAX(LastModified)) <= 1;"
cur.execute(tabsSameHourQuery)

userVolumeInfoQuery = "SELECT udc.TabID, udc.UserID, e.EmailAddress, " \
                        + " (CASE WHEN u.BillingCountry IS NOT NULL THEN u.BillingCountry ELSE u.IPCountry END)," \
                        + " replace(convert(varchar, udc.StartDate, 106), ' ', '-') , ud.volumeid, v.ImageID, v.Title, v.Attribution, ud.DisplayTitle, ud.Name" \
                        + " FROM Tempdb..#tmp_user_volumes_tabs_ud_counts udc" \
                        + " INNER JOIN Tempdb..#tmp_user_volumes_tabs_ud ud ON udc.TabID = ud.TabID and udc.UserID = ud.UserID" \
                        + " INNER JOIN ShortCover_Report.dbo.Volumes v ON ud.volumeid = v.VolumeID" \
                        + " INNER JOIN ShortCover_Report.dbo.User_email e ON udc.UserID = e.UserID" \
                        + " INNER JOIN ShortCover_Report.dbo.Users u ON udc.UserID = u.UserID" \
                        + " WHERE e.EmailSetting = 1" \
                        + " AND udc.StartDate BETWEEN '%s'" % yesterdaysDatetime + " and '%s'" % todaysDatetime \
                        + " ORDER BY udc.UserID, udc.TabID, udc.StartDate desc"

cur.execute(userVolumeInfoQuery)
rs = cur.fetchall()

formattedDate = "%02d" % now.month + "%02d" % now.day + str(now.year)
csvfile = "ios_" + formattedDate + ".csv"
csvfilename = "/home/iislogs/reports/%s" % str(csvfile)
csvfilehandle = open(csvfilename, 'wb')

failedTransactionsHandle = csv.writer(csvfilehandle, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

recordlist = ['IOS_DATE', 'IOS_EMAIL', 'IOS_COUNTRY', 'IOS_ACTIVATION_GAME1', 'IOS_URL1', 'IOS_TITLE1', 'IOS_AUTHOR1', 'IOS_IMAGE1', 'IOS_ACTIVATION_GAME2', 'IOS_URL2', 'IOS_TITLE2', 'IOS_AUTHOR2', 'IOS_IMAGE2', 'IOS_ACTIVATION_GAME3','IOS_URL3', 'IOS_TITLE3', 'IOS_AUTHOR3', 'IOS_IMAGE3', 'IOS_ACTIVATION_GAME4','IOS_URL4', 'IOS_TITLE4', 'IOS_AUTHOR4', 'IOS_IMAGE4', 'OPT_IN']
failedTransactionsHandle.writerow(recordlist[:24])
recordlist = []

lastTabid = ""
lastUserid = ""

# keep track of users as it was decided to not have dup emails sent out
usersProcessed = []

for row in rs:

	userid = str(UUID(bytes_le = row[1]))
    tabid = str(UUID(bytes_le = row[0]))

    emailAddress = str(row[2])

    country = str(row[3])

    if country is None:
        continue

        startDate = row[4]

        volumeid = b64encode(row[5]).rstrip("=")
        imageid = b64encode(row[6]).rstrip("=")
        title = str(row[7])
        author = str(row[8])
        displayTitle = str(row[9])

        if (tabid != lastTabid or userid != lastUserid):

            if len(recordlist) >= 23: # limit each user/tab-game session to 4 volumes

            	if userid not in usersProcessed:
                    if len(recordlist) > 23:
                		recordlist[23] = 1
                	else:
                		recordlist.append(1)
                    failedTransactionsHandle.writerow(recordlist[:24])
                    usersProcessed.append(userid)

                recordlist = []
                recordlist.append(startDate)
                recordlist.append(emailAddress)
                recordlist.append(country)

        recordlist.append(displayTitle)
        recordlist.append(volumeid)
        recordlist.append(title)
        recordlist.append(author)
        recordlist.append(imageid)

        lastTabid = tabid
        lastUserid = userid

if len(recordlist) >= 23:
	if userid not in usersProcessed:
		if len(recordlist) > 23:
            recordlist[23] = 1
        else:
            recordlist.append(1)
        failedTransactionsHandle.writerow(recordlist[:24])

csvfilehandle.close()

# sftp the csv file over to cheetahs server
host = "NotRealHost.com"
username = "NotRealUsername"
password = "NotRealPassword"
path = "autoproc/%s" % str(csvfile)

port = 22
transport = paramiko.Transport((host, port))
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)
localpath = csvfilename
sftp.put(localpath, path)
sftp.close()
transport.close()

if os.path.exists(csvfilename):
	print "csv file exists in correct path"
else:
	print "csv file does NOT exist in correct path"

#archive the csv file
mvcmd = "mv %s" % csvfilename + " %s" % archivePath
os.popen(mvcmd)