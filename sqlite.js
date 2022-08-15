var fs      = require('fs');
var extend  = require('extend');
var util    = require('util');
var knex    = require('knex');
var async   = require('async');

function SqliteAdapter(opts) {
  extend(this, opts);
}

SqliteAdapter.prototype.connect = function (cb) {
  if (this.knex) return cb();
  this.knex = knex({
    client: 'sqlite3',
    connection: {
      filename: this.path || ':memory:',
    },
    debug: false,
    useNullAsDefault: true,
    pool: {
      min: 1,
      max: 1
    },
    refreshIdle: false,
    acquireConnectionTimeout: this.acquireConnectionTimeout || 200,
  });
  var self = this;
  this._upsertQueue = async.queue(function (properties, cb) {
    var keys = Object.keys(properties);
    var values = keys.map(function (k) {
      return properties[k];
    });
    var sql = 'INSERT OR REPLACE INTO ' + self.tableName + ' (' + keys.join(',') + ') VALUES (' + values.map(function (x) { return '?'; }).join(',') + ')';
    self.knex.raw(sql, values).then(function () { cb(); }).catch(cb);
  })
  cb();
};

SqliteAdapter.prototype.upsert = function (properties, cb) {
  this._upsertQueue.push(properties, cb);
};

SqliteAdapter.prototype.close = function (cb) {
  if (this.knex && this.knex.client) this.knex.client.pool.destroy();
  setImmediate(cb);
};

module.exports = SqliteAdapter;
