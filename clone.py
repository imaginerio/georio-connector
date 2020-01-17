import datetime
import math
import os
import re
import requests
import psycopg2
from psycopg2.extras import NamedTupleCursor

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

GEOMS = {
  'point': 'MULTIPOINT',
  'line': 'MULTILINESTRING',
  'poly': 'MULTIPOLYGON'
}

SKIP_TABLES = [
  'basemapextentspoly',
  'mapextentspoly',
  'viewconespoly',
  'aerialextentspoly',
  'planextentspoly',
  'surveyextentspoly'
]

def loadData(table, date=None):
  layerName = re.sub(r"(point|line|poly)", "", table)
  m = re.search(r"(point|line|poly)", table)
  feature = tableName(m.group(0))
  print('CREATING LAYER FOR ' + table)
  l = requests.post('http://localhost:5000/api/v1/make/layer', {
    'geometry': feature,
    'title': layerName
  })
  layer = l.json()['response']

  print('CREATING TYPES FROM ' + table)
  q = "SELECT COALESCE(type, '{}') AS type FROM {}.{}_evw GROUP BY type".format(layerName, os.environ.get('DBSCHEMA'), table)
  remote.execute(q)
  types = remote.fetchall()
  type_dict = {}
  for t in types:
    typeName = t._asdict()['type']
    ft = requests.post('http://localhost:5000/api/v1/make/type/' + layer, { 'title': typeName })
    type_dict[typeName] = ft.json()['response']

  print('LOADING DATA FROM ' + table)
  q = """SELECT
      name,
      firstyear,
      lastyear,
      COALESCE(type, '{}') AS type,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM {}.{}_evw""".format(layerName, os.environ.get('DBSCHEMA'), table)
  if date:
    q += " WHERE last_edited_date > %s OR created_date > %s"
  remote.execute(q, (date, date))
  results = remote.fetchall()

  years = []
  if len(results) > 0:
    print('INSERTING ' + str(len(results)) + ' ROWS INTO ' + feature)
    s = requests.Session()
    for r in results:
      if r[-1] != 'EMPTY':
        data = r._asdict()
        body = {
          "type": type_dict[data['type']],
          "geometry": feature,
          "dataType": "wkt",
          "data": {
            "geometry": data['geom'],
            "properties": {
              "name": data['name'],
              "firstyear": data['firstyear'],
              "lastyear": data['lastyear'],
            }
          }
        }
        r = s.post(os.environ.get('APIHOST') + '/api/v1/make/feature', json=body)
  return years

# Feteching remote tables
def getTables():
  remote.execute("SELECT viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
  tables = remote.fetchall()
  return list(map(lambda t: re.sub(r"_evw$", "", t[0]), tables))

def tableName(g):
  g = "polygon" if g == "poly" else g
  return g

if __name__ == "__main__":
  tables = getTables()
  for table in tables:
    if not table in SKIP_TABLES:
      loadData(table)
