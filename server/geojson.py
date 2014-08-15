#! /usr/bin/python
# pycoact/server/geojson.py
# Copyright 2013, Trinity College Computing Center
# Last modified: 22 August 2013

import json
import sqlite3
import re
import os
import sys

class GeojsonServer(object):
	def __init__(self, filename, tablename):
		self.conn = sqlite3.connect(filename)
		self.conn.row_factory = sqlite3.Row
		self.tablename = tablename
		self.debug_level = 0

	def debug(self, level, message):
		if self.debug_level >= level:
			sys.stderr.write("GeojsonServer: %s\n" % message)

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

	def handle_request(self, data_handle, username):
		try:
			if os.environ["REQUEST_METHOD"] == "GET":
				result = self.load(os.environ["QUERY_STRING"])
			elif os.environ["REQUEST_METHOD"] == "POST":
				data = json.load(data_handle)
				result = self.save(data, username)
			else:
				raise AssertionError
			return json.dumps(result, separators=(',',':'))
		except Exception as e:
			import traceback, sys
			sys.stderr.write(traceback.format_exc(sys.exc_info()[2]))
			return json.dumps({"error":str(e)})

	def load(self, query_string):
		self.debug(1, "load(%s)" % query_string)

		m = re.match("^pulled_version=([-\d]+)$", query_string)
		if not m:
			raise AssertionError
		tver = int(m.group(1))

		cursor = self.conn.cursor()
		qres = cursor.execute("select * from %s where tver > ?" % self.tablename, (tver,))
		features = []
		for row in qres:
			feature = json.loads(row['data'])
			feature['id'] = row['id']
			feature['version'] = row['version']
			features.append(feature)
			tver = max(tver, row['tver'])
		return {"type":"FeatureCollection","features":features,"repository":{"pulled_version":tver}}

	def save(self, data, username):
		self.debug(1, "save(-, %s)" % username)

		cursor = self.conn.cursor()
		tver = self.table_version()
		tver += 1
		result = []
		assert data['type'] == 'FeatureCollection', data['type']
		for feature in data['features']:
			id = feature.get('id')
			version = feature.get('version')
			if id is not None:
				del feature['id']
				del feature['version']
			as_json = json.dumps(feature,separators=(',',':'))

			if id is not None:
				cursor.execute("update %s set version=version+1, tver=?, user=?, data=? where id=?" % self.tablename, (tver, username, as_json, id))
				cursor.execute("select version from %s where id = ?" % self.tablename, (id,))
				version = cursor.fetchone()[0]
				result.append((id, version))
			else:
				cursor.execute("insert into %s (version, tver, user, data) values (1,?,?,?)" % self.tablename, (tver, username, as_json))
				result.append((cursor.lastrowid, 1))

		self.conn.commit()
		return result

if __name__ == "__main__":
	import sys
	repo = GeojsonServer(sys.argv[1], sys.argv[2])
	repo.create()

