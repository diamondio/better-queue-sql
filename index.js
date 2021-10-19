var extend = require('extend');
var uuid   = require('uuid');
var util   = require('util');

function SqlStore(opts) {
  opts = opts || {};
  opts.tableName = opts.tableName || 'tasks';
  extend(this, opts);

  var dialect = opts.dialect || 'sqlite';
  if (dialect === 'sqlite') {
    var Adapter = require('./sqlite');
    this.adapter = new Adapter(opts);
  } else if (dialect === 'postgres') {
    var Adapter = require('./postgres');
    this.adapter = new Adapter(opts);
  } else if (dialect === 'mysql') {
    var Adapter = require('./mysql');
    this.adapter = new Adapter(opts);
  } else {
    throw new Error("Unhandled dialect: " + dialect);
  }
  this.dialect = dialect;
}

// http://stackoverflow.com/questions/11532550/atomic-update-select-in-postgres
var takeNextN = function (first) {
  return function (n, cb) {
    var self = this;
    var subquery = function (fields, n) {
      return self.adapter.knex(self.tableName).select(fields).where('lock', '').orderBy('priority', 'DESC').orderBy('added', first ? 'ASC' : 'DESC').limit(n);
    };
    if (self.dialect == 'mysql') {
      var innerSubquery = subquery;
      subquery = function (fields, n) {
        return self.adapter.knex.select(fields).from(innerSubquery(fields, n).as('tmp'));
      }
    }
    var lockId = uuid.v4();
    self.adapter.knex(self.tableName)
      .where('lock', '').andWhere('id', 'in', subquery(['id'], n))
      .update({ lock: lockId })
      .then(function (numUpdated) {
        var val = numUpdated > 0 ? lockId : '';
        cb(null, val);
        return val;
      }).catch(cb);
  };
};

SqlStore.prototype.connect = function (cb) {
  var self = this;
  self.adapter.connect(function (err) {
    if (err) return cb(err);
    var sql = util.format("CREATE TABLE IF NOT EXISTS %s ", self.tableName);
    var dialect = self.dialect;
    if (dialect === 'sqlite') {
      sql += "(id TEXT UNIQUE, lock TEXT, task TEXT, priority NUMERIC, added INTEGER PRIMARY KEY AUTOINCREMENT)";
    } else if (dialect === 'postgres') {
      sql += "(id TEXT UNIQUE, lock TEXT, task TEXT, priority NUMERIC, added SERIAL PRIMARY KEY)";
    } else if (dialect === 'mysql') {
      sql += "(id VARCHAR(191) UNIQUE, `lock` TEXT, task TEXT, priority NUMERIC, added INTEGER PRIMARY KEY AUTO_INCREMENT)";
    } else {
      throw new Error("Unhandled dialect: " + dialect);
    }
    return self.adapter.knex.raw(sql).then(function () {
      return self.adapter.knex(self.tableName).count('*').where('lock', '').then(function (rows) {
        var row = rows[0];
        row = row ? row['count'] || row['count(*)'] : 0;
        cb(null, row);
        return row;
      });
    }).catch(cb);
  });
};

SqlStore.prototype.getTask = function (taskId, cb) {
  this.adapter.knex(this.tableName).where('id', taskId).andWhere('lock', '').then(function (rows) {
    if (!rows.length) return cb();
    var row = rows[0];
    try {
      var savedTask = JSON.parse(row.task);
    } catch (e) {
      return cb('failed_to_deserialize_task');
    }
    cb(null, savedTask);
    return savedTask;
  }).catch(cb);
};

SqlStore.prototype.deleteTask = function (taskId, cb) {
  this.adapter.knex(this.tableName).where('id', taskId).del().then(function () { cb(); return taskId; }).catch(cb);
};

SqlStore.prototype.putTask = function (taskId, task, priority, cb) {
  try {
    var serializedTask = JSON.stringify(task);
  } catch (e) {
    return cb('failed_to_serialize_task');
  }
  this.adapter.upsert({ id: taskId, task: serializedTask, priority: priority || 1, lock: '' }, cb);
};

SqlStore.prototype.takeFirstN = takeNextN(true);
SqlStore.prototype.takeLastN = takeNextN(false);

SqlStore.prototype.getLock = function (lockId, cb) {
  this.adapter.knex(this.tableName).select(['id', 'task']).where('lock', lockId).then(function (rows) {
    var tasks = {};
    rows.forEach(function (row) {
      tasks[row.id] = JSON.parse(row.task);
    })
    cb(null, tasks);
    return tasks;
  }).catch(cb);
};

SqlStore.prototype.getRunningTasks = function (cb) {
  this.adapter.knex(this.tableName).select(['id', 'task', 'lock']).then(function (rows) {
    var tasks = {};
    rows.forEach(function (row) {
      if (!row.lock) return;
      tasks[row.lock] = tasks[row.lock] || [];
      tasks[row.lock][row.id] = JSON.parse(row.task);
    })
    cb(null, tasks);
    return tasks;
  }).catch(cb);
};

SqlStore.prototype.releaseLock = function (lockId, cb) {
  this.adapter.knex(this.tableName).where('lock', lockId).del().then(function () { cb(); return lockId; }).catch(cb);
};

SqlStore.prototype.close = function (cb) {
  if (this.adapter) return this.adapter.close(cb);
  cb();
};

module.exports = SqlStore;
