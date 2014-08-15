#! /usr/bin/python
# CGI frontend for the shared table server
# Last modified: 17 August 2013

import sys
import os
import codecs
import re

sys.path.insert(0, '..')	# above public_html/
from pycoact.server.table import SharedTableServer
from pycoact.server.geojson import GeojsonServer

try:
	sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
	sys.stderr = codecs.getwriter('utf-8')(sys.stderr)
	
	m = re.match('/([a-z0-9_]+)/([a-z0-9_]+)\.([a-z0-9]+)$', os.environ['PATH_INFO'])
	assert m, "Invalid PATH_INFO"
	db_name = m.group(1)
	tablename = m.group(2)
	tabletype = m.group(3)

	if tabletype == "stbcsv":
		table = SharedTableServer("../shared_tables/%s.db" % db_name, tablename, tabletype)
		table.debug_level = 1
		mime_type = "application/xml"
	elif tabletype == "geojson":
		table = GeojsonServer("../shared_tables/%s.db" % db_name, tablename)
		table.debug_level = 1
		mime_type = "application/json"
	else:
		raise ValueError("invalid table type: %s" % tabletype)

	response = table.handle_request(sys.stdin, os.environ['REMOTE_USER'])

	sys.stdout.write("Content-Type: %s\n" % mime_type)
	sys.stdout.write("\n")
	sys.stdout.write(response)

except Exception as e:
	import traceback
	(e_type, e_value, e_traceback) = sys.exc_info()
	message = traceback.format_exc(e_traceback)
	sys.stderr.write("%s\n" % message)
	sys.stdout.write("Content-Type: text/plain\n")
	sys.stdout.write("Status: 500 Request failed\n")
	sys.stdout.write("\n")
	sys.stdout.write(message)

