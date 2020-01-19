import os
import re
import shapely.wkt
from shapely.geometry import mapping
import fiona
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

PROPERTIES = {
  'name': 'str',
  'firstyear': 'int',
  'lastyear': 'int',
  'type': 'str'
}

def loadData(table, date=None):
  layerName = re.sub(r"(point|line|poly)", "", table)
  m = re.search(r"(point|line|poly)", table)
  feature = tableName(m.group(0))

  schema = {
    'geometry': feature,
    'properties': PROPERTIES
  }

  print('LOADING DATA FROM ' + table)
  q = """SELECT
      name,
      firstyear,
      lastyear,
      COALESCE(type, '{}') AS type,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM {}.{}_evw""".format(layerName, os.environ.get('DBSCHEMA'), table)
  remote.execute(q, (date, date))
  results = remote.fetchall()

  if len(results) > 0:
    print('CREATING SHAPEFILE WITH ' + str(len(results)) + ' ROWS INTO ' + feature)
    with fiona.open('shapefiles/' + layerName + '.shp', 'w', 'ESRI Shapefile', schema=schema) as c:
      for r in results:
        if r[-1] != 'EMPTY':
          data = r._asdict()
          geom = shapely.wkt.loads(data['geom'])
          c.write({
            'geometry': mapping(geom),
            'properties': {
              'name': 'test',
              'firstyear': 1,
              'lastyear': 2,
              'type': 'type',
            }
          })

# Feteching remote tables
def getTables():
  remote.execute("SELECT viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
  tables = remote.fetchall()
  return list(map(lambda t: re.sub(r"_evw$", "", t[0]), tables))

def tableName(g):
  if g == "poly":
    return "Polygon"
  if g == "line":
    return "LineString"
  return "Point"

if __name__ == "__main__":
  tables = getTables()
  for table in tables:
    if not table in SKIP_TABLES:
      loadData(table)
