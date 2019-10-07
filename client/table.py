# pycoact/client/table.py
# Copyright 2013--2017, Trinity College Computing Center
# Last modified: 18 January 2017

import xml.etree.cElementTree as ET
import urllib2
import os

#=============================================================================
# Client Library
#=============================================================================

# Thrown for errors during syncronization
class SharedTableError(Exception):
	pass

# Thrown if the local and server formats differ
class SharedTableFormatError(SharedTableError):
	pass

class SharedTable:
	def __init__(self, local_filename, table_format="raw", debug=0):
		self.local_filename = local_filename
		self.table_format = table_format
		self.debug_level = debug
		self.debug(1, "SharedTable.__init__()")

		# Load the local store into an ElementTree
		self.xml = ET.parse(self.local_filename)

		# Find the remote repository
		self.url = self.xml.find("repository/url").text
		#for i in self.xml.getroot():
		#	print "<%s>" % i.tag
		self.xml_pulled_version = self.xml.find("repository/pulled_version")
		realm = self.xml.find("repository/realm").text
		username = self.xml.find("repository/username").text
		password = self.xml.find("repository/password").text

		# Prepare to authenticate oneself.
		auth_handler = urllib2.HTTPDigestAuthHandler()
		auth_handler.add_password(
			uri=self.url,
			realm=realm,
			user=username,
			passwd=password
			)
		self.urlopener = urllib2.build_opener(auth_handler)
		#urllib2.install_opener(self.urlopener)

		# Find row containers
		self.xml_conflict_rows = self.find_or_create("conflict_rows")
		self.xml_rows = self.find_or_create("rows")
		self.xml_new_rows = self.find_or_create("new_rows")

		# These keep track of the data which the application has pulled out 
		# of the local store.
		self.read_rows = None
		self.read_conflicts = None
		self.read_new_rows = None

		self.new_table = False

	# Locate the specified container tag and return a reference to it.
	# If there is no such container tag, create one and return a reference
	# to the newly-created tag.
	def find_or_create(self, tag):
		self.debug(2, "Searching for <%s>" % tag)
		obj = self.xml.find(tag)
		if obj == None:
			self.debug(1, "Creating <%s>" % tag)
			obj = ET.SubElement(self.xml.getroot(), tag)
			obj.text = '\n'
			obj.tail = '\n'
		return obj

	#====================================================
	# For debugging
	#====================================================
	def debug(self, level, message):
		if self.debug_level >= level:
			print message

	def dump(self):
		if self.debug_level > 0:
			import sys
			assert self.xml
			print "====== Local Store ======"
			self.xml.write(sys.stdout)
			print

	#====================================================
	# Find all of the <row>s in a particular container
	# and return them in a hash keyed by the value of
	# the id attribute.
	#====================================================
	def index_rows(self, container):
		hash = {}
		for i in list(container):
			assert i.tag == "row"
			id = int(i.get("id"))
			hash[id] = i
		return hash

	#====================================================
	# Send an XML request to the server and receive
	# an XML response.
	#====================================================
	def post_xml(self, xml):
		data = ET.tostring(xml, encoding='utf-8')
		self.debug(1, "====== POSTed XML ======")
		self.debug(1, data)

		req = urllib2.Request(self.url, data, {'Content-Type':'application/xml'})
		try:
			#http = urllib2.urlopen(req)
			http = self.urlopener.open(req)
			resp_text = http.read()
			if resp_text == "":
				raise SharedTableError("HTTP response is empty.")
		except urllib2.HTTPError as e:
			raise SharedTableError("HTTP request failed: %s" % str(e))
		except Exception as e:
			raise SharedTableError(str(e))

		self.debug(1, "====== Response XML ======")
		self.debug(1, resp_text)

		return ET.XML(resp_text)

	#====================================================
	# Save the current state of the local store to file.
	#====================================================
	def save(self):
		self.debug(1, "SharedTable.save(): save to \"%s\"..." % self.local_filename)

		# MS-DOS naming scheme
		(base, ext) = os.path.splitext(self.local_filename)
		temp = "%s.tmp" % base
		backup = "%s.bak" % base
		if os.path.exists(backup):		# remove
			os.remove(backup)

		# Unix naming scheme
		temp = "%s.tmp" % self.local_filename
		backup = "%s~" % self.local_filename

		self.xml.write(temp, encoding='utf-8')
		if os.path.exists(self.local_filename):
			if os.path.exists(backup):
				os.remove(backup)
			os.rename(self.local_filename, backup)
		os.rename(temp, self.local_filename)

	#====================================================
	# Dump the local repository
	#====================================================
	def clear_local_store(self):
		self.debug(1, "SharedTable.clear_local_store()")
		for row in list(self.xml_conflict_rows):
			self.xml_conflict_rows.remove(row)
		for row in list(self.xml_rows):
			self.xml_rows.remove(row)
		for row in list(self.xml_new_rows):
			self.xml_new_rows.remove(row)
		self.xml_pulled_version.text = '0'

	#====================================================
	# Pull the latest changes down from the server.
	#====================================================
	def pull(self):
		self.debug(1, "SharedTable.pull()")

		# Build XML request
		top = ET.Element('request')
		top.text = '\n'
		child = ET.SubElement(top, 'type')
		child.text = 'pull'
		child.tail = '\n'
		child = ET.SubElement(top, 'pulled_version')
		child.text = self.xml_pulled_version.text
		child.tail = '\n'

		# Send request and parse the response
		resp = self.post_xml(top)

		# Index the rows already in our copy.
		conflict_rows_by_id = self.index_rows(self.xml_conflict_rows)
		rows_by_id = self.index_rows(self.xml_rows)

		# Take the received rows and use them to update our local copy
		count_changes = 0
		count_conflicts = 0
		for row in resp.find('rows'):
			assert row.tag == 'row'
			id = int(row.get('id'))
			version = row.get('version')
			self.debug(2, "Received row (id=%d, version=%s): %s" % (id, version, row.text))
			assert self.table_format != "stbcsv" or id != 0 or int(version) == 1, "Row with ID 0 may not advance beyond version 1."

			# already known to be in conflict
			if conflict_rows_by_id.has_key(id):
				self.debug(2, "  Known conflict")
				#count_conflicts += 1	# do this below only if also a change
				existing = conflict_rows_by_id[id]
				if version != existing.get('version'):			# If there were furthur changes,
					existing.attrib['version'] = version		# accept them.
					existing.text = row.text
					count_changes += 1
					count_conflicts += 1

			# If this row is in the repository,
			elif rows_by_id.has_key(id):
				existing = rows_by_id[id]
				if id == 0 and self.table_format == "stbcsv":
					if existing.text != row.text:
						raise SharedTableFormatError
				elif version == existing.get('version'):
					self.debug(2, "  Not changed on server")
				else:
					self.debug(2, "  Changed on server")
					count_changes += 1
					if existing.attrib.has_key('modified'):		# new conflict
						self.debug(1, "    new conflict")
						count_conflicts += 1
						self.xml_conflict_rows.append(row)
					else:										# non-conflicting change
						self.debug(2, "    updated")
						existing.attrib['version'] = version
						existing.text = row.text

			# completely new row
			else:			
				self.debug(2, "  New row from server")
				self.xml_rows.append(row)
				count_changes += 1

		# Copy the version number from the response to the local store.
		self.xml_pulled_version.text = resp.find('version').text

		assert count_changes >= count_conflicts, "count_changes=%d, count_conflicts=%d" % (count_changes, count_conflicts)
		return count_changes, count_conflicts

	@staticmethod
	def add_row(parent, id, version, data):
		child = ET.SubElement(parent, 'row')
		child.tail = '\n'
		child.attrib = {
			'id':id,
			'version':str(version),
			}
		child.text = data

	#====================================================
	# Push any modified recoreds up to the server.
	#====================================================
	def push(self):
		self.debug(1, "SharedTable.push()")

		count_changes = 0
		count_conflicts = 0

		# Build XML request
		top = ET.Element('request')
		top.text = '\n'
		child = ET.SubElement(top, 'type')
		child.text = 'push'
		child.tail = '\n'

		# Push any changes to existing rows
		req_rows = ET.SubElement(top, 'rows')
		req_rows.text = '\n'
		req_rows.tail = '\n'
		for row in list(self.xml_rows):
			id = int(row.get('id'))
			if id == 0 and self.table_format == 'stbcsv':
				assert int(row.get('version')) == 1, "Row with ID 0 must never be modified."
				self.add_row(req_rows, row.get('id'), 1, row.text)
			elif row.attrib.has_key('modified'):
				self.debug(2, "Row %s is modified: %s" % (row.get('id'), row.text))
				count_changes += 1
				self.add_row(req_rows, row.get('id'), int(row.get('version')) + 1, row.text)

		# Push new rows
		req_new_rows = ET.SubElement(top, 'new_rows')
		req_new_rows.text = '\n'
		req_new_rows.tail = '\n'
		for row in list(self.xml_new_rows):
			self.debug(2, "New row: %s" % row.text)
			count_changes += 1
			child = ET.SubElement(req_new_rows, 'row')
			child.text = row.text
			child.tail = '\n'

		# Make the request only if it is non-empty
		if count_changes > 0:
			count_changes_accepted = 0

			# Send request and parse the response
			resp = self.post_xml(top)
	
			result = resp.find('result').text
			if result == "FORMAT_CONFLICT":
				raise SharedTableFormatError
			elif result != "OK":
				raise SharedTableError

			# Remove the modified attribute from rows for which the change
			# was accepted and bump the version number.
			rows_by_id = self.index_rows(self.xml_rows)
			for r_row in list(resp.find("modified_rows")):
				assert r_row.tag == "row"
				id = int(r_row.get('id'))
				self.debug(1, "Row successfully modified: %d" % id)
				row = rows_by_id[id]
				del row.attrib['modified']
				row.attrib['version'] = str(int(row.get('version')) + 1)
				count_changes_accepted += 1
	
			# Accept the IDs which the server has assigned to the new rows
			# and move them to the end of the main list of rows.
			r_new_rows = list(resp.find("new_rows"))
			for row in list(self.xml_new_rows):
				assert row.tag == "row"
				r_row = r_new_rows.pop(0)
				assert r_row.tag == "row"
				id = r_row.get('id')
				self.debug(1, "New row received id: %s" % id)
				child = ET.SubElement(self.xml_rows, "row")
				child.attrib = {'id':id, 'version':'1'}
				child.text = row.text
				child.tail = "\n"
				self.xml_new_rows.remove(row)
				count_changes_accepted += 1

			count_conflicts = int(resp.find("conflict_count").text)

			# Make sure all changes are accounted for.
			# count_changes -- the number we submitted
			# count_changes_accepted -- number for which server return new version
			# count_conflicts -- number of conflicts which server claimed
			self.debug(1, "count_changes: %d" % count_changes)
			self.debug(1, "count_changes_accepted: %d" % count_changes_accepted)
			self.debug(1, "count_conflicts: %d" % count_conflicts)
			assert count_changes == (count_changes_accepted + count_conflicts)
	
			# An optimization to (most of the time) prevent the changes we push
			# from coming right back at us the next time we pull.
			#	
			# If one or more of our changes took and the version number that the
			# table now has on the server is one greater than the last one that
			# we list pulled, then we and only we made it increase. That means
			# that we can safely bump the version number on our side without
			# doing a pull.
			if count_changes_accepted > 0:
				tver = int(resp.find('version').text)
				if tver == (int(self.xml_pulled_version.text) + 1):
					self.debug(1, "No other pushes since last pull, bumping tver.")
					self.xml_pulled_version.text = str(tver)
				else:
					self.debug(1, "There has been an intervening push, leaving tver.")
			else:
				self.debug(1, "No changes were made.")

		assert count_changes >= count_conflicts, "count_changes=%d, count_conflicts=%d" % (count_changes, count_conflicts)
		return count_changes, count_conflicts

