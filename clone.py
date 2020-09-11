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
  'immigrantbusinesspoint',
  'basemapextentspoly',
  'mapextentspoly',
  'viewconeextentspoly',
  'planextentspoly',
  'surveyextentspoly'
]

VIEWCONE_TABLES = [
  'viewconespoly'
]

VISUAL_TABLES = [
  'aerialextentspoly',
  'mapextentspoly',
  'planextentspoly',
]

PROPERTIES = {
  'name': 'str',
  'namealt': 'str',
  'firstyear': 'int',
  'lastyear': 'int',
  'type': 'str'
}

def makeShapefile(table, results, feature, properties):
  schema = {
    'geometry': feature,
    'properties': properties
  }

  if len(results) > 0:
    print('CREATING SHAPEFILE WITH ' + str(len(results)) + ' ROWS INTO ' + feature)
    with fiona.open('shapefiles/' + table + '.shp', 'w', 'ESRI Shapefile', encoding='utf-8', schema=schema) as c:
      for r in results:
        if r[-1] != 'EMPTY':
          data = r._asdict()
          geom = shapely.wkt.loads(data.pop('geom'))
          c.write({
            'geometry': mapping(geom),
            'properties': data
          })

def loadData(table, date=None):
  layerName = re.sub(r"(point|line|poly)", "", table)
  m = re.search(r"(point|line|poly)", table)
  feature = tableName(m.group(0))
  namealt = "REGEXP_REPLACE(namealt, '\D*', '')" if table == 'roadsline' else "''"

  print('LOADING DATA FROM ' + table)
  q = """SELECT
      name,
      {} AS namealt,
      COALESCE(firstyear, LEFT(firstdate::TEXT, 4)::INT) AS firstyear,
      COALESCE(lastyear, LEFT(lastdate::TEXT, 4)::INT) AS lastyear,
      COALESCE(type, '{}') AS type,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM {}.{}_evw
    WHERE LENGTH(shape::TEXT) < 5000000""".format(namealt, layerName, os.environ.get('DBSCHEMA'), table)
  remote.execute(q, (date, date))
  results = remote.fetchall()
  makeShapefile(table, results, feature, PROPERTIES)
  

def loadViewcone(table):
  properties = {
    'ss_id': 'str',
    'ssc_id': 'str',
    'creditline': 'str',
    'creator': 'str', 
    'date': 'str',
    'title': 'str',
    'latitude': 'float',
    'longitude': 'float',
    'firstyear': 'int',
    'lastyear': 'int'
  }
  print('LOADING DATA FROM ' + table)
  q = """SELECT
      ss_id,
      ssc_id,
      creditline,
      creator,
      date,
      title,
      latitude::FLOAT,
      longitude::FLOAT,
      COALESCE(firstyear, LEFT(firstdate::TEXT, 4)::INT) AS firstyear,
      COALESCE(lastyear, LEFT(lastdate::TEXT, 4)::INT) AS lastyear,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM {}.{}_evw""".format(os.environ.get('DBSCHEMA'), table)
  remote.execute(q)
  results = remote.fetchall()
  makeShapefile(table, results, 'Polygon', properties)

def loadVisual(table):
  properties = {
    'ss_id': 'str',
    'ssc_id': 'str',
    'creditline': 'str',
    'creator': 'str', 
    'date': 'str',
    'title': 'str',
    'firstyear': 'int',
    'lastyear': 'int'
  }
  print('LOADING DATA FROM ' + table)
  q = """SELECT
      ss_id,
      ssc_id,
      creditline,
      creator,
      date,
      title,
      COALESCE(firstyear, LEFT(firstdate::TEXT, 4)::INT) AS firstyear,
      COALESCE(lastyear, LEFT(lastdate::TEXT, 4)::INT) AS lastyear,
      ST_AsText(ST_Envelope(ST_Transform(shape, 4326))) AS geom
    FROM {}.{}_evw""".format(os.environ.get('DBSCHEMA'), table)
  remote.execute(q)
  results = remote.fetchall()
  makeShapefile(table, results, 'Polygon', properties)

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
    if table in VIEWCONE_TABLES:
      loadViewcone(table)
    elif table in VISUAL_TABLES:
      loadVisual(table)
    elif not table in SKIP_TABLES:
      loadData(table)
