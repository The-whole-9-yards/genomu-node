var msgpack = require('msgpack-buf');
var fs = require('fs');
var net = require('net');
var put = require('put');

var modules = fs.readdirSync((process.env['GENOMU_PATH'] || '../genomu/apps/genomu') + '/priv/modules').
              map(function(file) {
                return JSON.parse(fs.readFileSync('../genomu/apps/genomu/priv/modules/' + file));
              });

var api = function(args, hdr) {
  if (args < 2) {
      return function() {
         return Buffer.concat([hdr,
                              msgpack.pack(arguments[0])]);
      };
  } else {
      return function() {
        return Buffer.concat([hdr,
                              msgpack.pack(arguments)]);
      };
  }
}

for (i in modules) {
  exports[modules[i].name] = {};
  for (j in modules[i].operations) {
    var mid = modules[i].id, oid = modules[i].operations[j].id;
    var hdr = Buffer.concat([msgpack.pack(mid),
                             msgpack.pack(oid)]);
    exports[modules[i].name][modules[i].operations[j].name] = api(modules[i].operations[j].args, hdr);
  }
}

var decode = function(data, options) {
    var p = msgpack.unpack(data);
    if (p == true || p == null) {
      return p;
    }
    var vsn = msgpack.unpack(data.slice(msgpack.pack(p).length));
    var txn = msgpack.unpack(data.slice(msgpack.pack(p).length + msgpack.pack(vsn).length));
    var obj;
    if (typeof options.vsn !== 'undefined') {
      obj = {value: p, vsn: vsn}
    }
    if (typeof options.txn !== 'undefined') {
      obj = {value: p, txn: txn}
    }
    if (typeof options.vsn !== 'undefined' && typeof options.txn !== 'undefined') {
      obj = {value: p, vsn: vsn, txn: txn}
    }
    if (typeof options.vsn === 'undefined' && typeof options.txn === 'undefined') {
      obj = p;
    }

    return obj;
}

var setf = function(addr, op, cb, options) {
  options = options || {}
  var cmd = Buffer.concat([msgpack.pack(this.channel), msgpack.pack(addr), msgpack.pack(1), op]);
  var buf = put().word32be(cmd.length).buffer();

  this.connection.channels[this.channel] = this.connection.channels[this.channel] || [];
  this.connection.channels[this.channel].push(function(data) {
    cb.apply(this, [decode(data, options)]);
  });

  this.connection.write(Buffer.concat([buf, cmd]));
}

var getf = function(addr, op, cb, options) {
  options = options || {}
  var cmd = Buffer.concat([msgpack.pack(this.channel), msgpack.pack(addr), msgpack.pack(0), op]);
  var buf = put().word32be(cmd.length).buffer();

  this.connection.channels[this.channel] = this.connection.channels[this.channel] || [];
  this.connection.channels[this.channel].push(function(data) {
    cb.apply(this, [decode(data, options)]);
  });

  this.connection.write(Buffer.concat([buf, cmd]));
}

var applyf = function(addr, op, cb, options) {
  options = options || {}
  var cmd = Buffer.concat([msgpack.pack(this.channel), msgpack.pack(addr), msgpack.pack(2), op]);
  var buf = put().word32be(cmd.length).buffer();

  this.connection.channels[this.channel] = this.connection.channels[this.channel] || [];
  this.connection.channels[this.channel].push(function(data) {
    cb.apply(this, [decode(data, options)]);
  });

  this.connection.write(Buffer.concat([buf, cmd]));
}

var commitf = function() {
  options = {}
  var cmd = Buffer.concat([msgpack.pack(this.channel), msgpack.pack(true)]);
  var buf = put().word32be(cmd.length).buffer();

  this.connection.channels[this.channel] = this.connection.channels[this.channel] || [];
  this.connection.channels[this.channel].push(function(data) {
    cb.apply(this, [decode(data, options)]);
  });

  this.connection.write(Buffer.concat([buf, cmd]));
}

var Channel = function(connection, id) {
  this.set = setf;
  this.get = getf;
  this.apply = applyf;
  this.commit = commitf;
  this.connection = connection;
  this.channel = id;
}

var startChannel = function() {
  this.channel += 1;
  var cmd = Buffer.concat([msgpack.pack(this.channel), msgpack.pack({})]);
  var buf = put().word32be(cmd.length).buffer();
  this.write(Buffer.concat([buf, cmd]));
  var channel = new Channel(this, this.channel);
  return channel;
}

exports.connect = function(instance, cb) {
 cb = cb || function() {};
 instance = instance || {};
 var opts = instance;
 opts.port = opts.port || 9101;
 var conn = net.connect(opts,
              function() {
                 cb.call(conn, conn);
              });
 conn.on('data', function(data) {
   var sz = data.readUInt32BE(0);
   var channel = msgpack.unpack(data.slice(4));
   var cb = conn.channels[channel].shift();
   var tail = data.slice(4 + msgpack.pack(channel).length);
   cb.call(conn, tail);
 });
 conn.channel = 0;
 conn.channels = {};
 conn.begin = startChannel;
};