import datetime
import math
import os
import re
import psycopg2

remote_conn = psycopg2.connect(
  "host='{}' dbname='{}' user='{}' password='{}'"
    .format(
      os.environ.get('DBHOST'),
      os.environ.get('DBNAME'),
      os.environ.get('DBUSER'),
      os.environ.get('DBPASS')
    )
)
remote = remote_conn.cursor()
local_conn = psycopg2.connect(
  "host='localhost' dbname='{}'"
    .format(os.environ.get('LOCALDB'))
)
local = local_conn.cursor()

GEOMS = {
  'point': 'MULTIPOINT',
  'line': 'MULTILINESTRING',
  'poly': 'MULTIPOLYGON'
}

SKIP = [
  'basemapextentspoly',
  'landextentspoly'
]

VISUAL = [
  'basemapextentspoly',
  'mapextentspoly',
  'viewconespoly',
  'aerialextentspoly',
  'planextentspoly'
]

def createTable(table, geom):
  print('CREATING ' + table)
  local.execute('DROP TABLE IF EXISTS "{}"'.format(table))
  local.execute("""CREATE TABLE "{}" (
    "gid" SERIAL,
    "remoteid" int,
    "name" text,
    "layer" text,
    "firstdispl" int,
    "lastdispla" int,
    "featuretyp" text,
    "stylename" text,
    "geom" geometry({}, 4326),
    PRIMARY KEY ("gid")
  )""".format(table, geom))
  local.execute("""CREATE INDEX {}_geom_idx
    ON "{}"
    USING GIST (geom);""".format(table, table))
  local_conn.commit()

def loadVisual(table):
  print('LOADING VISUAL DATA FROM ' + table)
  if table == 'viewconespoly':
    layer = 'viewsheds'
    coords = ''
  else:
    layer = re.sub(r"extentspoly$", 's', table)
    coords = 'NULL AS'
  q = """SELECT
      '{}' AS layer,
      ss_id,
      creator,
      ssc_id AS repository,
      firstyear,
      lastyear,
      notes,
      ST_AsText(ST_Transform(shape, 4326)) AS geom,
      NULL AS uploaddate,
      {} latitude,
      {} longitude,
      creditline,
      title,
      date
    FROM {}.{}_evw""".format(layer, coords, coords, os.environ.get('DBSCHEMA'), table)
  remote.execute(q)
  results = remote.fetchall()

  if len(results) > 0:
    table = 'viewsheds' if table == 'viewconespoly' else 'mapsplans'
    print('INSERTING ' + str(len(results)) + ' ROWS INTO ' + table)
    local.execute('TRUNCATE {} RESTART IDENTITY'.format(table))
    for r in results:
      local.execute("""INSERT INTO "{}" VALUES (
        DEFAULT,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        ST_GeomFromText(%s, 4326),
        %s,
        %s,
        %s,
        %s,
        %s,
        %s)""".format(table), r)
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
      subtype,
      stylename,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM {}.{}_evw""".format(layer, os.environ.get('DBSCHEMA'), table)
  if date:
    q += " WHERE last_edited_date > %s OR created_date > %s"
  remote.execute(q, (date, date))
  results = remote.fetchall()

  years = []
  if len(results) > 0:
    print('INSERTING ' + str(len(results)) + ' ROWS INTO ' + feature)
    for r in results:
      if r[-1] != 'EMPTY':
        years.append([
          r[2] or int(math.floor(r[4] / 10000)), 
          r[3] or int(math.floor(r[5] / 10000))
        ])
        local.execute("""INSERT INTO "{}" VALUES (
          DEFAULT,
          %s,
          %s, 
          %s,
          %s,
          %s,
          %s,
          %s,
          ST_Multi(ST_GeomFromText(%s, 4326))
        )
        ON CONFLICT (gid) DO UPDATE
          SET
            remoteid = %s,
            name = %s,
            layer = %s,
            firstdispl = %s,
            lastdispla = %s,
            featuretyp = %s,
            stylename = %s,
            geom = ST_Multi(ST_GeomFromText(%s, 4326))""".format(feature), r + r)
    local_conn.commit()
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
  return 'base' + g

if __name__ == "__main__":
  for g in GEOMS:
    createTable(tableName(g), GEOMS[g])

  tables = getTables()
  for table in tables:
    if not table in SKIP:
      if table in VISUAL:
        loadVisual(table)
      else:
        loadData(table)

  local_conn.autocommit = True
  for g in GEOMS:
    local.execute('VACUUM ANALYZE "{}"'.format(tableName(g)))

  updateLog('clone')
