var extend = require('extend');

function MysqlAdapter(opts) {
  extend(this, opts);
}

MysqlAdapter.prototype.connect = function (cb) {
  if (this.knex) return cb();
  var connection = {
    database: this.dbname || 'template1',
    host: this.host || 'localhost',
    port: this.port || 3306,
    user: this.username || 'root',
    password: this.password || '',
    charset: this.charset || 'utf8'
  };
  this.knex = require('knex')({
    client: 'mysql',
    connection: connection,
    debug: false,
    useNullAsDefault: true,
    pool: {
      min: 1,
      max: 8
    }
  });
  return cb();
};

MysqlAdapter.prototype.upsert = function (properties, cb) {
  var keys = Object.keys(properties);
  var values = keys.map(function (k) {
    return properties[k];
  });
  var sql = 'REPLACE INTO ' + this.tableName +
    ' (' + keys.map(function (key) { return '`' + key + '`'; }).join(',') +
    ') VALUES (' + values.map(function (x) { return '?'; }).join(',') + ')';
  this.knex.raw(sql, values)
    .then(function () { cb(); })
    .catch(cb);
};

MysqlAdapter.prototype.close = function (cb) {
  if (this.knex && this.knex.client) this.knex.client.pool.destroy();
  setImmediate(cb);
};

module.exports = MysqlAdapter;
