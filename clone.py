import datetime
import os
import re
import psycopg2

remote_conn = psycopg2.connect(
  "host='128.42.130.18' dbname='hw_houston' user='{}' password='{}'"
    .format(os.environ.get('DBUSER'), os.environ.get('DBPASS'))
)
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

def loadData(table, date=None):
  print('LOADING DATA FROM ' + table)
  q = """SELECT
      objectid,
      name,
      firstyear,
      lastyear,
      firstdate,
      lastdate,
      type,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM uilvim.{}_evw""".format(table)
  if date:
    q += " WHERE last_edited_date > %s OR created_date > %s"
  remote.execute(q, (date, date))
  results = remote.fetchall()

  if len(results) > 0:
    print('INSERTING ' + str(len(results)) + ' ROWS INTO ' + table)
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
        )
        ON CONFLICT (objectid) DO UPDATE
          SET
            objectid = %s,
            name = %s,
            firstyear = %s,
            lastyear = %s,
            firstdate = %s,
            lastdate = %s,
            type = %s,
            geom = ST_Multi(ST_GeomFromText(%s, 4326))""".format(table), r + r)
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
if __name__ == "__main__":
  tables = getTables()
  for table in tables:
    if not table in VISUAL:
      createTable(table)
      loadData(table)
      quit()

  updateLog('clone')
