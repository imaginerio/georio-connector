const dbgeo = require('dbgeo');
const db = require('./db');

const geoms = {
  point: 'MULTIPOINT',
  line: 'MULTILINESTRING',
  poly: 'MULTIPOLYGON'
}

db.remote.many("SELECT viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
  .then((views) => {
    let tables = views.map(v => v.viewname);
    const loader = tables.map((t) => {
      console.log(`LOADING ${t}`);
      return db.remote.many(`SELECT
        objectid,
        name,
        firstyear,
        lastyear,
        firstdate,
        lastdate,
        type,
        ST_AsText(ST_Transform(shape, 4326)) AS geom
      FROM uilvim.${t}`).then((geo) => {
        console.log(`LOADED ${t}`);
        const table = t.replace(/_evw$/gm, '');
        return db.local.any(`SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = '${table}'`)
          .then((table_exists) => {
            let q = `SELECT 'Hello World!'`;
            if (table_exists.length === 0) {
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
                );`
            }
            return db.local.any(q).then(() => {
              const features = geo.filter(g => g.geom != 'EMPTY');
              return db.local.tx((local) => {
                const updates = features.map((g) => {
                  return local.none(`INSERT INTO ${table} VALUES (
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
                  });
                });
                console.log(`UPDATING ${table}`);
                return local.batch(updates);
              });
            });
        }).catch((e) => {
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