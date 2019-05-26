from clone import getTables, loadData, updateLog, local, VISUAL

def getLastUpdate():
  local.execute("SELECT date FROM update_log ORDER BY date DESC")
  return local.fetchone()[0]

if __name__ == "__main__":
  # Get last update
  last = getLastUpdate()

  # Feteching remote tables
  tables = getTables()

  for table in tables:
    if not table in VISUAL:
      years = loadData(table, last)
      print(years)

  updateLog('update')
