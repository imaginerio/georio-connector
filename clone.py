import datetime
import math
import os
import re
import requests
import psycopg2
from psycopg2.extras import NamedTupleCursor

remote_conn = psycopg2.connect(
  "host='{}' dbname='{}' user='{}' password='{}'"
    .format(
      os.environ.get('DBHOST'),
      os.environ.get('DBNAME'),
      os.environ.get('DBUSER'),
      os.environ.get('DBPASS')
    )
)
remote = remote_conn.cursor(cursor_factory=NamedTupleCursor)
local_conn = psycopg2.connect("host='localhost' dbname='houston'")
local = local_conn.cursor()

GEOMS = {
  'point': 'MULTIPOINT',
  'line': 'MULTILINESTRING',
  'poly': 'MULTIPOLYGON'
}

VISUAL = [
  'basemapextentspoly',
  'mapextentspoly',
  'viewconeextentspoly',
  'aerialextentspoly',
  'planextentspoly'
]

def createTable(table, geom):
  print('CREATING ' + table)
  local.execute('DROP TABLE IF EXISTS "{}"'.format(table))
  local.execute("""CREATE TABLE "{}" (
    "objectid" int,
    "name" text,
    "layer" text,
    "firstyear" int,
    "lastyear" int,
    "firstdate" int,
    "lastdate" int,
    "type" text,
    "geom" geometry({}, 4326),
    PRIMARY KEY ("objectid")
  )""".format(table, geom))
  local.execute("""CREATE INDEX {}_geom_idx
    ON "{}"
    USING GIST (geom);""".format(table, table))
  local_conn.commit()

def loadData(table, date=None):
  layer = re.sub(r"(point|line|poly)", "", table)
  m = re.search(r"(point|line|poly)", table)
  feature = tableName(m.group(0))
  print('LOADING DATA FROM ' + table)
  q = """SELECT
      objectid,
      name,
      '{}' AS layer,
      firstyear,
      lastyear,
      firstdate,
      lastdate,
      type,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM uilvim.{}_evw""".format(layer, table)
  if date:
    q += " WHERE last_edited_date > %s OR created_date > %s"
  remote.execute(q, (date, date))
  results = remote.fetchall()

  years = []
  if len(results) > 0:
    print('INSERTING ' + str(len(results)) + ' ROWS INTO ' + feature)
    for r in results:
      if r[-1] != 'EMPTY':
        feature_data = { feature: r._asdict() }
        r = requests.post('http://localhost:5000/api/v1/create-feature/' + feature + '/wkt/', feature_data)
  return years

# Feteching remote tables
def getTables():
  remote.execute("SELECT viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
  tables = remote.fetchall()
  return list(map(lambda t: re.sub(r"_evw$", "", t[0]), tables))

def updateLog(type):
  local.execute("""CREATE TABLE IF NOT EXISTS update_log (
    "id" serial,
    "type" text,
    "date" timestamp without time zone,
    PRIMARY KEY ("id")
  )""")
  local.execute("""INSERT INTO update_log VALUES (
    DEFAULT,
    %s,
    %s
  )""", (type, datetime.datetime.now()))
  local_conn.commit()

def tableName(g):
  g = "polygon" if g == "poly" else g
  return g

if __name__ == "__main__":
  for g in GEOMS:
    createTable(tableName(g), GEOMS[g])

  tables = getTables()
  for table in tables:
    if not table in VISUAL:
      loadData(table)

  local_conn.autocommit = True
  for g in GEOMS:
    local.execute('VACUUM ANALYZE "{}"'.format(tableName(g)))

  updateLog('clone')
