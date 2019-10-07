#! /usr/bin/python
# pycoact/server/table.py
# Copyright 2013, 2014, 2015, Trinity College Computing Center
# Last modified: 15 February 2015
#

import sqlite3
import xml.etree.cElementTree as ET
import StringIO
import sys

class BadRequest(Exception):
	pass

class SharedTableServer:

	def __init__(self, filename, tablename, tabletype):
		self.conn = sqlite3.connect(filename)
		#self.conn.row_factory = sqlite3.Row	# not used yet
		self.tablename = tablename
		self.tabletype = tabletype
		self.debug_level = 0

	# Send debugging messages to the web server error log
	def debug(self, level, message):
		if self.debug_level >= level:
			sys.stderr.write("SharedTableServer: %s\n" % message)

	# This function extracts the current version of the table.
	#	
	# Before any rows have been added, the table is at version 0. Every time
	# a client pushes changes we bump the table version up and store it in
	# each added or modified row. Thus, for each row, the tver is the table
	# version in which it was last modified. Clients can request only those
	# rows which were modified in table versions later than the last one which
	# they downloaded.
	#
	# We do not store the table version anywhere. Instead we use a SQL query
	# to find the highest table version in any row.
	#
	def table_version(self):
		cursor = self.conn.cursor()
		cursor.execute("select max(tver) from %s" % self.tablename)
		version = cursor.fetchone()[0]
		version = 0 if version is None else int(version)
		self.debug(1, "Current table version: %d" % version)
		return version

	# Create the database table
	def create(self):
		cursor = self.conn.cursor()
		cursor.execute("create table %s (id integer primary key, version integer, tver integer, user varchar, data text)" % self.tablename)
		cursor.execute("create index %s_idx on %s (tver)" % (self.tablename, self.tablename))

	# Client is pulling down new changes made by other clients.
	# Client will supply a version number. We will return the first row
	# (so that the client can verify that the format has not changed)
	# and all rows which have a version later than the one specified.
	def handle_request_pull(self, req):

		pulled_version = int(req.find("pulled_version").text)
		self.debug(1, "Pull after version %d" % pulled_version)

		cursor = self.conn.cursor()
		cursor.execute("select * from %s where tver > ? or id = 0 order by id" % self.tablename, [pulled_version])

		# Create <response>
		top = ET.Element('response')
		top.text = '\n'

		# Add a <version> child to <response> which holds the current tver.
		child = ET.SubElement(top, 'version')
		child.text = str(self.table_version())
		child.tail = '\n'

		# Add a <rows> container to the <response>.
		xml_rows = ET.SubElement(top, 'rows')
		xml_rows.text = '\n'
		xml_rows.tail = '\n'

		# For each row returned by the SQL query, added a <row> to <rows>.
		for row in cursor:
			id, version, tver, user, data = row
			child = ET.SubElement(xml_rows, 'row')
			child.attrib = {'id':str(id),
							'version':str(version),
							}
			child.text = data
			child.tail = '\n'

		return ET.tostring(top)
	
	# Client is pushing up its own new changes.
	def handle_request_push(self, req, req_username):
		mods = []
		news = []
		conflict_count = 0
		result = 'OK'

		tver = self.table_version()
		tver += 1

		cursor = self.conn.cursor()
		cursor.execute("begin transaction")

		# Modification of existing rows
		modified_rows = list(req.find('rows'))
		for row in modified_rows:
			assert row.tag == 'row'
			id = int(row.get('id'))
			version = int(row.get('version'))
			text = row.text
			self.debug(2, "%d, %d, %s" % (id, version, text))

			# In stbcsv tables, the first row is treated specially. It can never be altered
			# due to a request from the client. The client always sends it as if it were
			# modified so that we can make sure the formats are the same. The client
			# always assumes we already have it, so if we don't we must quietly create it
			# when first a client does a push.
			if id == 0 and self.tabletype == 'stbcsv':
				assert version == 1, "Row with ID 0 must remain at version 1"
				cursor.execute("select data from %s where id = ?" % self.tablename, (0,))
				data = cursor.fetchone()
				if data is None:
					cursor.execute("insert into %s (id, version, tver, user, data) values (?, ?, ?, ?, ?)" % self.tablename, [0, 1, tver, req_username, text])
				else:
					if text != data[0]:
						result = "FORMAT_CONFLICT"
						break

			# With other rows which the client claims to have modified, we try to updating them
			# using a where clause which contains the number of the version on which the client
			# claims to have based its modifications. If it has been changed since the client
			# pulled, nothing will be modified and we will declare a conflict.
			else:
				assert version >= 1
				cursor.execute("update %s set version=?, tver=?, user=?, data=? where id=? and version=?" % self.tablename, [version, tver, req_username, text, id, version-1])
				if cursor.rowcount == 0:		# version not as expected
					self.debug(1, "conflict")
					conflict_count += 1
				else:
					mods.append(id)

		# Addition of new rows
		new_rows = list(req.find('new_rows'))
		if len(new_rows) > 0:
			cursor.execute("select max(id) from %s" % self.tablename)
			id = cursor.fetchone()[0]
			id = -1 if id is None else int(id)
			self.debug(1, "Last row was: %d" % id)

			for row in new_rows:
				assert row.tag == 'row'
				id += 1
				text = row.text
				self.debug(2, "new row %d: %s" % (id, text))
				cursor.execute("insert into %s (id, version, tver, user, data) values (?, ?, ?, ?, ?)" % self.tablename, [id, 1, tver, req_username, text])
				news.append(id)

		self.debug(1, "Submitted modified rows: %d" % len(modified_rows))
		self.debug(1, "Submitted new rows: %d" % len(new_rows))
		self.debug(1, "Accepted modified rows: %d" % len(mods))
		self.debug(1, "Accepted new rows: %d" % len(news))

		# If we didn't manage to actually change anything, move the
		# table version number back to what it was.
		if len(mods) == 0 and len(news) == 0:
			tver -= 1

		# Format XML response	
		xml_top = ET.Element('response')
		xml_top.text = '\n'

		# Error?
		child = ET.SubElement(xml_top, 'result')
		child.text = result
		child.tail = '\n'

		# What is the table version when this change has been committed?
		child = ET.SubElement(xml_top, 'version')
		child.text = str(tver)
		child.tail = '\n'

		child = ET.SubElement(xml_top, 'conflict_count')
		child.text = str(conflict_count)
		child.tail = '\n'

		child = ET.SubElement(xml_top, 'modified_rows')
		child.text = '\n'
		child.tail = '\n'
		for id in mods:
			gchild = ET.SubElement(child, 'row')
			gchild.attrib['id'] = str(id)
			gchild.tail = '\n'

		child = ET.SubElement(xml_top, 'new_rows')
		child.text = '\n'
		child.tail = '\n'
		for id in news:
			gchild = ET.SubElement(child, 'row')
			gchild.attrib['id'] = str(id)
			gchild.tail = '\n'

		return ET.tostring(xml_top)

	# Parse the XML request, dispatch it to the proper handler,
	# and send the handler's response back to the client.
	def handle_request(self, in_fh, username):
		assert username

		req = ET.parse(in_fh)
		action = req.find("type").text
		self.debug(1, "Request: %s" % action)

		if action == "pull":
			response = self.handle_request_pull(req)
		elif action == "push":
			response = self.handle_request_push(req, username)
		else:
			raise BadRequest("unrecognized request type")

		self.conn.commit()

		return response

if __name__ == "__main__":
	import sys
	if len(sys.argv) != 4:
		sys.stderr.write("Usage: %s: <filename> <tablename> <tabletype>\n")
		sys.exit(1)
	repo = SharedTableServer(sys.argv[1], sys.argv[2], sys.argv[3])
	repo.create()


