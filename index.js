const db = require('./db');

const offset = 10000;
const geoms = {
  point: 'MULTIPOINT',
  line: 'MULTILINESTRING',
  poly: 'MULTIPOLYGON',
};

function loadRemote(table, start) {
  return db.remote.many(`SELECT
      objectid,
      name,
      firstyear,
      lastyear,
      firstdate,
      lastdate,
      type,
      ST_AsText(ST_Transform(shape, 4326)) AS geom
    FROM uilvim.${table}_evw
    LIMIT ${start + offset} OFFSET ${start}`).then((geo) => {
    console.log(`LOADED ${table} FROM ${start}`);
    const features = geo.filter(g => g.geom !== 'EMPTY');
    return db.local.tx((local) => {
      const updates = features.map(g => local.none(`INSERT INTO ${table} VALUES (
        $/objectid/,
        $/name/,
        $/firstyear/,
        $/lastyear/,
        $/firstdate/,
        $/lastdate/,
        $/type/,
        ST_Multi(ST_GeomFromText($/geom/, 4326))
      )
      ON CONFLICT (objectid) 
      DO NOTHING;`, g).catch((e) => {
        console.log(e.error);
      }));
      console.log(`UPDATING ${table}`);
      return local.batch(updates).then(() => {
        if (geo.length === offset) {
          return loadRemote(table, start + offset);
        }
        return null;
      });
    });
  });
}

db.remote.many("SELECT viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
  .then((views) => {
    const tables = views.map(v => v.viewname);
    const loader = tables.map((t) => {
      console.log(`LOADING ${t}`);
      const table = t.replace(/_evw$/gm, '');
      return db.local.any(`SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = '${table}'`)
        .then((tableExists) => {
          let q = 'SELECT \'Hello World!\'';
          if (tableExists.length === 0) {
            const geom = table.match(/(point|line|poly)/gmi);
            console.log(`CREATING ${table}`);
            q = `CREATE TABLE ${table} (
                  "objectid" int,
                  "name" text,
                  "firstyear" int,
                  "lastyear" int,
                  "firstdate" int,
                  "lastdate" int,
                  "type" text,
                  "geom" geometry(${geoms[geom]}, 4326),
                  PRIMARY KEY ("objectid")
              );`;
          }
          return db.local.any(q).then(() => loadRemote(table, 0)).catch((e) => {
            console.log(e);
          });
        }).catch((e) => {
          console.log(e);
        });
    });
    Promise.resolve(loader);
  }).catch((e) => {
    console.log(e);
  });
