#! /usr/bin/python
# shared_table_modify.py
# Last modified: 10 August 2013

import sys
import sqlite3

import StringIO
import csv

# FIXME: untested
def csv_split(line):
	r = csv.reader([line.encode('utf-8'))
	return [unicode(cell, 'utf-8') for cell in r.next()]

# FIXME: untested
def csv_join(row):
	f = StringIO.StringIO()
	w = csv.writer(f)
	w.writerow([cell.encode('utf-8') for cell in row])
	w = None
	return f.getvalue().decode('utf-8').strip()

# FIXME: untested
def server_add_column(filename, tablename, col_after, col_new):
	conn = sqlite3.connect(filename)
	cursor = conn.cursor()
	
	cursor.execute("begin transaction")
	
	# Read table in
	cursor.execute("select id, data from %s" % tablename)
	rows = []
	for id, data in cursor:
		print id, data
		r = csv.reader([data])
		data_split = r.next()
		rows.append([id, csv_split(data)])
	
	# Find position of new column and insert it
	row1 = rows[0][1]
	pos = row1.index(col_after) + 1
	pos = find_pos(rows[0][1], col_after, col_new)
	if pos < len(row1) and row1[pos] == col_new:
		print "Column was already added."
		return

	value = col_new
	for id, data_split in rows:
		data_split.insert(pos, value)
		value = ""
	
	# Write modified table out
	for id, data_split in rows:
		data = csv_join(data_split)
		print id, data
		cursor.execute("update %s set data = ? where id = ?" % tablename, (data, id))
		print cursor.rowcount
	
	conn.commit()

