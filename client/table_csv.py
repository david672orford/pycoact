#! /usr/bin/python
# pycoact/client/table.py
# Copyright 2013, 2014, Trinity College Computing Center
# Last modified: 23 August 2014

import xml.etree.cElementTree as ET
import pyapp.csv_unicode as csv
from pycoact.client.table import SharedTable, SharedTableError, SharedTableFormatError

class SharedTableCSVConflict:
	def __init__(self, index, obj):
		assert index > 0, "Conflict on row 0 should not be possible."
		self.obj = obj
		self.index = index
		self.resolved = False

	# Return the index of the row in conflict and the row contents from the server.
	def get_row(self):
		reader = csv.reader([self.obj.text])
		row = reader.next()
		return (self.index, row)

	def resolve(self):
		assert not self.resolved
		self.resolved = True

	def disresolve(self):
		assert self.resolved
		self.resolved = False

class SharedTableCSV(SharedTable):
	def __init__(self, local_filename, debug=0):
		SharedTable.__init__(self, local_filename, "stbcsv", debug=debug)
		self.csv_rows = None
		self.csv_conflicts = None

	#====================================================
	# Return an object which will return all of the rows
	# in id order.
	# Remember the ids of the rows since the caller will
	# not receive them. Also remember which of the rows
	# have conflicts, since the caller may ask later by
	# calling get_conflicts().
	#====================================================
	def csv_reader(self):
		self.debug(1, "SharedTable.csv_reader()")

		rows = self.index_rows(self.xml_rows)
		conflict_rows = self.index_rows(self.xml_conflict_rows)
		self.csv_rows = []
		self.csv_conflicts = []
		self.csv_new_rows = []

		# CSV data that will need to be parsed
		data = []

		# Add those rows which are already on the server to data[].
		# Take special note of any that are in conflict with the server
		# versions.
		index = 0
		for key in sorted(rows.keys()):
			self.debug(1, "CSV server row: %s" % rows[key].text)
			self.csv_rows.append(rows[key])
			data.append(rows[key].text)
			if conflict_rows.has_key(key):
				self.csv_conflicts.append(SharedTableCSVConflict(index, conflict_rows[key]))
			index += 1

		# Add the new rows which we have not yet added to the server to the very end.
		for row in list(self.xml_new_rows):
			self.debug(2, "CSV new row: %s" % row.text)
			self.csv_new_rows.append(row)
			data.append(row.text)

		return csv.reader(data)

	#====================================================
	# Return an object to which the caller can writerow()
	# the rows return by csv_reader() as then now are.
	#====================================================
	def csv_writer(self):
		self.debug(1, "SharedTable.csv_writer()")

		# Make sure csv_reader() has been called.
		assert self.csv_rows is not None
		assert self.csv_conflicts is not None

		# The caller has previously told us if any conflicts will be resolved
		# by this write. Apply this stored up information to the local store.
		index = 0
		remain = []
		for conflict in self.csv_conflicts:
			if conflict.resolved:
				self.debug(2, "Conflict %d resolved" % index)
				# Change the version number of our copy of the row in order to indicate
				# that it is a modified copy of the conflicting row. (The modifictions
				# resolve the conflict.)
				self.csv_rows[conflict.index].attrib['version'] = conflict.obj.attrib['version']
				self.xml_conflict_rows.remove(conflict.obj)
			else:
				self.debug(2, "Conflict %d not resolved" % index)
				remain.append(conflict)
			index += 1
		self.csv_conflicts = remain

		self.csv_overall_index = 0
		self.csv_rows_index = 0
		self.csv_new_rows_index = 0

		# Create a CVS writer that will write lines by calling write() below.
		return csv.writer(self)

	def write(self, text):
		self.debug(1, "SharedTable.write(\"%s\")" % text)
		text = text.rstrip("\r\n")

		# Look at the index number of the row (not the ID).
		# It will fall into one of three possible ranges,
		# each of which requires a particular treatment.
		if self.csv_rows_index < len(self.csv_rows):				# Row is already on the server
			self.debug(2, "  row exists in repository")
			existing = self.csv_rows[self.csv_rows_index]
			if existing.text != text:
				self.debug(2, "    modified")
				existing.text = text
				existing.set('modified', '1')
			self.csv_rows_index += 1
		elif self.csv_new_rows_index < len(self.csv_new_rows):		# Not on server, but was already in local store
			self.debug(2, "  row exists in local store")
			existing = self.csv_new_rows[self.csv_new_rows_index]
			existing.text = text
			self.csv_new_rows_index += 1
		elif self.csv_overall_index == 0 and not self.new_table:	# Special case
			self.debug(2, "  Header row (when local store is empty)")
			# The first row of the table always specifies the column headings.
			# This special case prevents it from becoming a new row before we
			# can pull it from the repository. 
			child = ET.SubElement(self.xml_rows, 'row')
			child.attrib = {'id':'0', 'version':'1'}
			child.text = text
			child.tail = "\n"
		else:														# Entirely new
			self.debug(2, "  new row added to local store")
			child = ET.SubElement(self.xml_new_rows, 'row')
			child.text = text
			child.tail = "\n"
			# It is important to do this since there is no guarantee that the
			# user of this library will call csv_reader() before calling
			# csv_writer() again.
			self.csv_new_rows_index += 1
			self.csv_new_rows.append(child)

		self.csv_overall_index += 1

	#====================================================
	# Return the list of conflicts which were noted
	# when csv_reader() was preparing the data.
	#====================================================
	def get_conflicts(self):
		self.debug(1, "SharedTable.get_conflicts()")
		assert self.csv_conflicts is not None
		return self.csv_conflicts

	#====================================================
	# Add a column to the local copy of the shared table
	#====================================================
	def add_column(self, col_after, col_new):
		import StringIO
		
		def csv_split(line):
			return csv.reader([line]).next()

		def csv_join(row):
			f = StringIO.StringIO()
			csv.writer(f).writerow(row)
			return f.getvalue().strip()

		if self.csv_rows is not None:
			raise SharedTableError("add_column() must be called before csv_reader()")
	
		print "Adding new column %s after column %s..." % (col_new, col_after)
		xml_rows = list(self.xml.find("rows")) \
			+ list(self.xml.find("conflict_rows")) \
			+ list(self.xml.find("new_rows"))
		
		print " Existing first row:", xml_rows[0].text
		row = csv_split(xml_rows[0].text)
		pos = row.index(col_after) + 1

		if pos < len(row) and row[pos] == col_new:
			print " Column %s already exists at position %d." % (col_new, pos)
		else:
			print " Adding column %s at position %d..." % (col_new, pos)
			value = col_new
			for xml_row in xml_rows:
				row = csv_split(xml_row.text)
				row.insert(pos, value)
				xml_row.text = csv_join(row)
				value = ""

#=============================================================================
# Command line client which demonstrates use of above client library
#=============================================================================
if __name__ == "__main__":
	import sys
	util(sys.argv[0], sys.argv[1:], 1)

def util(progname, args):
	import codecs

	if len(args) < 3:
		print "Usage: %s import <filename.stb> <filename.csv>" % progname
		print "       (Adds rows from CSV file)"
		print "       %s export <filename.stb> <filename.csv>" % progname
		print "       (Writes all rows to CSV file)"
		print "       %s update <filename.stb> <filename.csv>" % progname
		print "       (Loads replacement rows from CSV file)"
		return 0

	subcommand, stb_filename, csv_filename = args
	if subcommand == "import":
		client = SharedTableCSV(stb_filename)
		#client.debug_level = 1

		reader = csv.reader(codecs.open(csv_filename, "rb", "utf-8"))

		# We can't call csv_writer() until we have called this.
		client.csv_reader()
	
		writer = client.csv_writer()

		for row in reader:
			writer.writerow(row)
	
		#client.push()
		client.save()
		return 0

	elif subcommand == "export":
		client = SharedTableCSV(stb_filename)
		#client.debug_level = 1

		writer = csv.writer(codecs.open(csv_filename, "wb", "utf-8"))

		reader = client.csv_reader()
		for row in reader:
			writer.writerow(row)

		return 0

	elif subcommand == "update":
		client = SharedTableCSV(stb_filename)
		#client.debug_level = 1

		reader = csv.reader(codecs.open(csv_filename, "rb", "utf-8"))

		dummy_reader = client.csv_reader()
		for row in dummy_reader:
			pass

		writer = client.csv_writer()
		for row in reader:
			writer.writerow(row)

		client.save()
		return 0

	else:
		sys.stderr.write("Unrecognized subcommand: %s\n" % subcommand)
		return 255

# end of file
