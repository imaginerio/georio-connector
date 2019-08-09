import datetime
import json
import math
import os
import re
import requests
import psycopg2
from psycopg2.extras import NamedTupleCursor
import shortuuid

remote_conn = psycopg2.connect(
  "host='{}' dbname='{}' user='{}' password='{}' options='-c statement_timeout=3000s'"
    .format(
      os.environ.get('DBHOST'),
      os.environ.get('DBNAME'),
      os.environ.get('DBUSER'),
      os.environ.get('DBPASS')
    )
)
remote_conn.set_client_encoding('UTF8')
remote = remote_conn.cursor(cursor_factory=NamedTupleCursor)
local_conn = psycopg2.connect("host='localhost' dbname='georio-store'")
local = local_conn.cursor()

GEOMS = {
  'point': 'MULTIPOINT',
  'line': 'MULTILINESTRING',
  'poly': 'MULTIPOLYGON'
}

SKIP_TABLES = [
  'immigrantbusinesspoint',
  'basemapextentspoly',
  'mapextentspoly',
  'viewconeextentspoly',
  'aerialextentspoly',
  'planextentspoly'
]

def loadData(table, metadata, date=None):
  m = re.search(r"(point|line|poly)", table)
  feature = tableName(m.group(0))
  metadata['geometry'] = feature
  metadata['remoteId'] = table
  layer_request = requests.post('http://localhost:5000/api/v1/layer/create/', json={ 'data': metadata })
  if layer_request.status_code == 200:
    layer = layer_request.json()['response']

    print('LOADING AND CREATING TYPES FROM ' + table)
    q = """SELECT type FROM uilvim.{}_evw GROUP BY type ORDER BY type""".format(table)
    remote.execute(q)
    types = remote.fetchall()
    type_dict = {}
    if len(types) > 0:
      for t in types:
        type_data = {}
        type_data['title'] = t._asdict()['type']
        type_data['remoteId'] = type_data['title'].lower().replace(' ', '-')
        if 'types' in metadata:
          type_data['minzoom'] = metadata['types'][type_data['remoteId']]
        type_request = requests.post('http://localhost:5000/api/v1/type/create/', json={ 'layer': layer, 'data': type_data })
        if type_request.status_code == 200:
          type_dict[type_data['title']] = type_request.json()['response']

      print('LOADING DATA FROM {}'.format(table))
      q = """SELECT
          objectid,
          name,
          firstyear,
          lastyear,
          TO_DATE(firstdate::TEXT, 'YYYYMMDD') AS firstdate,
          TO_DATE(lastdate::TEXT, 'YYYYMMDD') AS lastdate,
          type,
          ST_AsText(ST_Transform(shape, 4326)) AS geom
        FROM uilvim.{}_evw
        WHERE firstyear IS NOT NULL
          AND lastyear IS NOT NULL""".format(table)
      remote.execute(q, (date, date))
      results = remote.fetchall()

      years = []
      if len(results) > 0:
        print('INSERTING ' + str(len(results)) + ' ROWS INTO ' + feature)
        for r in results:
          if r[-1] != 'EMPTY':
            geojson = r._asdict()
            q = """INSERT INTO {} VALUES (
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              ST_Multi(ST_GeomFromText(%s, 4326)),
              ST_Transform(ST_Multi(ST_GeomFromText(%s, 4326)), 3857),
              NOW(),
              NOW()
            )""".format(feature + 's')
            local.execute(q, (
              shortuuid.uuid(),
              geojson['objectid'],
              geojson['name'],
              geojson['firstyear'],
              geojson['lastyear'],
              geojson['firstdate'],
              geojson['lastdate'],
              type_dict[geojson['type']],
              geojson['geom'],
              geojson['geom']
            ))
        local_conn.commit()

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
  with open('metadata.json') as json_file:
    metadata = json.load(json_file)
    tables = getTables()
    for table in tables:
      if not table in SKIP_TABLES:
        loadData(table, metadata[table])

  local_conn.autocommit = True
  for g in GEOMS:
    local.execute('VACUUM ANALYZE "{}s"'.format(tableName(g)))

  updateLog('clone')
