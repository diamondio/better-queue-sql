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
  var args = [properties.id, properties.lock, properties.task, properties.priority];
  this.knex.raw("REPLACE INTO " + this.tableName + " VALUES (?, ?, ?, ?)", args)
    .then(function () { cb(); })
    .error(cb);
};

MysqlAdapter.prototype.close = function (cb) {
  if (this.knex && this.knex.client) this.knex.client.pool.destroy();
  setImmediate(cb);
};

module.exports = MysqlAdapter;
