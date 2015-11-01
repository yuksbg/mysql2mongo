#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import math
import json
import optparse

from pymongo import MongoClient

import MySQLdb as mdb

parser = optparse.OptionParser()

parser.add_option('-t', '--tables', action="store", dest="tables", help="List of exported tables by comma ", default="")
parser.add_option('--host', action="store", dest="host", help="MySql host", default="127.0.0.1")
parser.add_option('-u', '--user', action="store", dest="user", help="MySql username", default="root")
parser.add_option('-p', '--password', action="store", dest="password", help="MySql password", default="")
parser.add_option('-d', '--database', action="store", dest="database", help="MySql database", default="")
parser.add_option('-m', '--mongo', action="store", dest="mongo", help="MongoDB connection string", default="mongodb://localhost:27017")
parser.add_option('--mongodb', action="store", dest="mongodb", help="MongoDB databse for exported data", default="exported")

options, args = parser.parse_args()


class Migrator:
	def __init__(self, options):
		self.limit = 100
		self.options = options

		self.make_connection()

		for table in options.tables.split(','):
			print "Processing %s \n" % table
			founded_rows = self.count_table(table)
			print "Founded %s rows" % founded_rows

			pages = int(math.ceil(founded_rows / self.limit))

			for el in range(0, pages):
				res = self.get_data(table, el * self.limit)
				self.mongo_db[table].insert(res)
			print

	def get_data(self, table_name, offset):
		sql = "select * from %s limit %s,%s" % (table_name, offset, self.limit)
		cur = self.con.cursor()
		cur.execute(sql)

		columns = cur.description
		result = [{columns[index][0]: str(column) for index, column in enumerate(value)} for value in cur.fetchall()]

		return result

	def make_connection(self):
		self.con = mdb.connect(self.options.host, self.options.user, self.options.password, self.options.database)
		mongo_client = MongoClient(self.options.mongo)
		self.mongo_db = mongo_client[self.options.mongodb]

	def count_table(self, table_name):
		cur = self.con.cursor()
		cur.execute("select count(*) from %s " % table_name)
		return int(cur.fetchall()[0][0])


if __name__ == "__main__":
	Migrator(options)