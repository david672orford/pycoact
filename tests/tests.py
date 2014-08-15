#! /usr/bin/python
# coding=utf-8
# pycoact/tests/test.py
# Last modified: 12 August 2013

import os
import sys
import thread
import BaseHTTPServer
import StringIO

sys.path.insert(1, "../..")
from pycoact.server.table import SharedTableServer
from pycoact.client.table_csv import SharedTableCSV

#=============================================================================
# Test HTTP server
#=============================================================================

test_db = "test_tables.db"

if os.path.exists(test_db):
	os.unlink(test_db)

class TestRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
	def do_POST(self):
		print ">>>POST %s" % self.path

		length = int(self.headers.getheader("content-length"))
		request_body_fd = StringIO.StringIO(self.rfile.read(length))

		table = SharedTableServer(test_db, "testtable", "csv")
		table.debug_level = 1

		try:
			response = table.handle_request(request_body_fd, "testuser")

			self.send_response(200)
			self.send_header("Content-Type", "application/xml")
			self.end_headers()
			self.wfile.write(response)

		except Exception as e:
			import traceback, sys
			(e_type, e_value, e_traceback) = sys.exc_info()
			message = traceback.format_exc(e_traceback)

			self.send_response(500)
			self.send_header("Content-Type", "text/plain")
			self.end_headers()
			self.wfile.write(message)

httpd = BaseHTTPServer.HTTPServer(("127.0.0.1", 8080), TestRequestHandler)
thread.start_new_thread(httpd.serve_forever, ())

#=============================================================================
# Run the tests
#=============================================================================

print "Client 1 is loading local store..."
client1 = SharedTableCSV("test_local_store.xml")
client1.debug_level = 1
client1.local_filename = "test_local_store_saved.xml"
client1.dump()
print

print "Client 1 is creating the shared table..."
if not client1.create_table():
	print "Drop testtable first."
	quit()
print

print "Client 1 is reading data (strictly a formality)..."
reader = client1.csv_reader()
for row in reader:
	print "Read row:", row
print

print "Client 1 is writing data back..."
writer = client1.csv_writer()
writer.writerow(["Name", "Age"])
writer.writerow(["David", "12"])
writer.writerow(["John", "13"])
writer.writerow(["Susan", "7"])
client1.dump()
print

print "Client 1 is pushing changes to server..."
client1.push()
client1.dump()
print

print "Client 2 is loading local store..."
client2 = SharedTableCSV("test_local_store.xml")
client2.debug_level = 1
client2.dump()
print

print "Client 2 is pulling changes..."
client2.pull()
print

print "Client 2 is reading data..."
reader = client2.csv_reader()
rows2 = []
for row in reader:
	print "Read row:", row
	rows2.append(row)
print

print "Client 2 is changing one row..."
rows2[2][1] = "15"
print

print "Client 2 is writing data back..."
writer = client2.csv_writer()
for row in rows2:
	writer.writerow(row)
print

print "Client 2 is pushing changes to server..."
client2.push()
print

print "Client 1 is reading data..."
reader = client1.csv_reader()
rows1 = []
for row in reader:
	print "Read row:", row
	rows1.append(row)
print

print "Client 1 is making a conflicting change..."
rows1[2][0] = u"Иван"
print

print "Client 1 is writing data back..."
writer = client1.csv_writer()
for row in rows1:
	writer.writerow(row)
print

print "Client 1 is pushing changes..."
client1.push()
print

print "Client 1 is pulling changes..."
client1.pull()
print

print "Client 1 is rereading data..."
reader = client1.csv_reader()
rows1 = []
for row in reader:
	print "Read row:", row
	rows1.append(row)
print

print "Conflicts:"
conflicts = client1.get_conflicts()
for conflict in conflicts:
	c_index, c_row = conflict.get_row()
	print c_index, c_row
print

print "Resolving conflict..."
rows1[2][1] = "15"
conflicts[0].resolve()
print

print "Client 1 is writing data back..."
writer = client1.csv_writer()
for row in rows1:
	writer.writerow(row)
print

print "Client 1 is pushing changes..."
client1.push()
print

print "Client 2 is pulling changes..."
client2.pull()
print

print "Client 2 is reading data..."
reader = client2.csv_reader()
rows2 = []
for row in reader:
	print "Read row:", row
	print "          %s, %s" % (row[0], row[1])
	rows2.append(row)
print

correct = [
	[u"Name", u"Age"],
	[u'David', u'12'],
	[u'Иван', u'15'],
	[u'Susan', u'7']
	]
if rows2 == correct:
	print "All tests passed."
else:
	print "Final results are not correct."
print

client1.save()
print

print "Following errors are irrelevant."
