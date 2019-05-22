import re
import psycopg2

remote_conn = psycopg2.connect("host='128.42.130.18' dbname='hw_houston' user='connector' password='SQ<r?6yT=.#cfm<H'")
remote = remote_conn.cursor()
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

def createTable(table):
  print('CREATING ' + table)
  m = re.search(r"(point|line|poly)", table)
  if m:
    geom = GEOMS[m.group(0)]
    local.execute("DROP TABLE IF EXISTS {}".format(table))
    local.execute("""CREATE TABLE {} (
      "objectid" int,
      "name" text,
      "firstyear" int,
      "lastyear" int,
      "firstdate" int,
      "lastdate" int,
      "type" text,
      "geom" geometry({}, 4326),
      PRIMARY KEY ("objectid")
    )""".format(table, geom))
  local_conn.commit()

def loadData(table):
  print('LOADING DATA FROM ' + table)
  remote.execute("""SELECT
      objectid,
      name,
      firstyear,
      lastyear,
      firstdate,
      lastdate,
      type,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM uilvim.{}_evw""".format(table))
  results = remote.fetchall()

  print('INSERTING DATA INTO ' + table)
  for r in results:
    if r[-1] != 'EMPTY':
      local.execute("""INSERT INTO {} VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        ST_Multi(ST_GeomFromText(%s, 4326))
      )""".format(table), r)
  local_conn.commit()

# Feteching remote tables
remote.execute("SELECT viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
tables = remote.fetchall()
tables = list(map(lambda t: t[0], tables))

for t in tables:
  table = re.sub(r"_evw$", "", t)
  if not table in VISUAL:
    createTable(table)
    loadData(table)
