#! /usr/bin/python
# pycoact/client/table_json.py
# Copyright 2013, Trinity College Computing Center
# Last modified: 12 August 2013

class SharedTableJSON(SharedTable):
	def __init__(self, dbname, tablename)
		SharedTable.__init__(self, dbname, tablename, "json")

		self.store = self.index_rows(self.xml_rows)
		self.next_neg_id = -1
		for row in self.xml_new:
			self.store[self.next_neg_id] = row
			self.next_neg_id -= 1

	def keys(self):
		return self.store.keys()

	def get(self, key, default=None):
		return json.loads(self.store.get(key, default).text)

	def __index__(self, key)
		return json.loads(self.index[key].text)

