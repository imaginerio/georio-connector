const promise = require('bluebird');

const isProduction = process.env.NODE_ENV === 'production';
if (!isProduction) require('dotenv').config(); // eslint-disable-line global-require,import/no-extraneous-dependencies

const options = {
  promiseLib: promise,
  error(err, e) {
    console.log(`ERROR: ${e.query}`);
  },
};

const pgp = require('pg-promise')(options);

const db = {
  local: pgp(process.env.LOCAL_DATABASE_URL),
  remote: pgp(process.env.REMOTE_DATABASE_URL),
};

module.exports = db;
