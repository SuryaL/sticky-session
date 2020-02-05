'use strict';

var cluster = require('cluster');
var util = require('util');
var net = require('net');
var ip = require('ip');

var debug = require('debug')('sticky:master');

function Master(workerCount, env) {
  net.Server.call(
    this,
    {
      pauseOnConnect: true
    },
    this.balance
  );

  this.env = env || {};

  this.seed = (Math.random() * 0xffffffff) | 0;
  this.workers = [];
  this.nextWorker = 0;
  this.workerCount = workerCount;
  this.algo = this.env.algo || 'ip';

  debug('master seed=%d', this.seed);

  this.once('listening', function() {
    debug('master listening on %j', this.address());

    for (var i = 0; i < workerCount; i++) this.spawnWorker();
  });
}
util.inherits(Master, net.Server);
module.exports = Master;

Master.prototype.hash = function hash(ip) {
  var hash = this.seed;
  for (var i = 0; i < ip.length; i++) {
    var num = ip[i];

    hash += num;
    hash %= 2147483648;
    hash += hash << 10;
    hash %= 2147483648;
    hash ^= hash >> 6;
  }

  hash += hash << 3;
  hash %= 2147483648;
  hash ^= hash >> 11;
  hash += hash << 15;
  hash %= 2147483648;

  return hash >>> 0;
};

Master.prototype.spawnWorker = function spawnWorker() {
  var worker = cluster.fork(this.env);

  var self = this;
  worker.on('exit', function(code) {
    debug('worker=%d died with code=%d', worker.process.pid, code);
    self.respawn(worker);
  });

  worker.on('message', function(msg) {
    // Graceful exit
    if (msg.type === 'close') self.respawn(worker);
    if (msg === 'ready') {
      console.log('ready');
      self.workers.push(worker);
    }
  });

  debug('worker=%d spawn', worker.process.pid);
};

Master.prototype.respawn = function respawn(worker) {
  var index = this.workers.indexOf(worker);
  if (index !== -1) this.workers.splice(index, 1);
  this.spawnWorker();
};

Master.prototype.balance = function balance(socket) {
  let index = 0;

  if (!this.workers.length) {
    socket.destroy();
    return;
  }
  if (this.algo == 'rr') {
    index = this.nextWorker;
    this.nextWorker = this.nextWorker >= this.workers.length - 1 ? 0 : this.nextWorker + 1;
  } else {
    var addr = ip.toBuffer(socket.remoteAddress || '127.0.0.1');
    var hash = this.hash(addr);
    index = hash % this.workers.length;
    debug('balacing connection %j', addr);
  }
  this.workers[index].send('sticky:balance', socket);
};
