#!/usr/bin/python

import vertica_python
import unicodecsv as csv
from optparse import OptionParser
import os
import getpass
import sys

parser = OptionParser()
parser.add_option("-f", dest="query_filename",
                  help="read query from FILE", metavar="FILE")
parser.add_option("-o", dest="output_filename",
                  help="write results to FILE", metavar="FILE")

parser.add_option("-H", "--host", dest="host", default="127.0.0.1")
parser.add_option("-p", "--port", dest="port", default=5434)
parser.add_option("-d", "--db", dest="database", default="analytics_raw")

parser.add_option("-u", dest="username", default=os.getlogin())

parser.add_option("--no_headers", dest="headers", default=True, action="store_false")

(options, args) = parser.parse_args()

if not options.query_filename:
  print "query filename required, not specified"
  parser.print_help()
  sys.exit(1)

if not options.output_filename:
  print "output filename required, not specified"
  parser.print_help()
  sys.exit(1)



password = getpass.getpass()

conn_info = {'host': options.host,
             'port': options.port,
             'user': options.username,
             'password': password,
             'database': options.database,
             # 10 minutes timeout on queries
             'read_timeout': 600,
             # default throw error on invalid UTF-8 results
             'unicode_error': 'strict'}

# using with for auto connection closing after usage
with vertica_python.connect(**conn_info) as connection:
  with open(options.output_filename, 'wb') as csvfile:
    writer = csv.writer(csvfile)
    query = open(options.query_filename).read()
    cur = connection.cursor()
    cur.execute(query)
    if options.headers:
      writer.writerow([d.name for d in cur.description])
    for row in cur.iterate():
      writer.writerow(row)
