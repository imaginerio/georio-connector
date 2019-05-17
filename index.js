const dbgeo = require('dbgeo');
const db = require('./db');

const geoms = {
  point: 'MULTIPOINT',
  line: 'MULTILINE',
  poly: 'MULTIPOLYGON'
}

db.remote.many(`SELECT viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'`)
  .then((views) => {
    const tables = views.map(v => v.viewname);
    const loader = tables.map((t) => {
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
        const table = t.replace(/_evw$/gm, '');
        db.local.any(`SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = '${table}'`)
        .then((table_exists) => {
          let q = `SELECT ''`;
          if (table_exists.length === 0) {
            const geom = table.match(/(point|line|poly)/gmi);
            q = `CREATE TABLE ${table} (
                  "id" serial,
                  "objectid" int,
                  "name" text,
                  "firstyear" int,
                  "lastyear" int,
                  "firstdate" int,
                  "lastdate" int,
                  "type" text,
                  "geom" geometry(${geoms[geom]}, 4326),
                  PRIMARY KEY ("id"),
                  UNIQUE ("objectid")
              );`
          }
          return db.local.any(q).then(() => {
            geo.map((g) => {
              return db.local.none(`INSERT INTO ${table} VALUES (
                DEFAULT,
                $/objectid/,
                $/name/,
                $/firstyear/,
                $/lastyear/,
                $/firstdate/,
                $/lastdate/,
                $/type/,
                ST_Multi(ST_GeomFromText($/geom/, 4326))
              )`, g)
            });
            db.local.task(geo);
          });
        });
      })
    });
    db.remote.task(loader);
  })