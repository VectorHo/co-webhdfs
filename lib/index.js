var _ = require('codash'),
  request = require('co-request').defaults({
    jar: true,
    agentOptions: {
      keepAlive: true
    }
  }),
  rawRequest = require('request').defaults({
    jar: true,
    agentOptions: {
      keepAlive: true
    }
  });

var strStandbyException = 'StandbyException';

var WebHdfsClient = module.exports = function(options) {
  // cookies承载对象
  this.jar = request.jar();

  this.options = _.defaults(options || {}, {
    protocol: 'http',
    user: 'hadoop',
    // token: 'delegation=token',
    namenode_port: 50070,
    namenode_host: 'localhost',
    path_prefix: '/webhdfs/v1'
  });

  var port = this.options.namenode_port,
    prefix = this.options.path_prefix;

  var hosts = this.options.namenode_hosts || this.options.namenode_host;
  if (!_.isArray(hosts)) hosts = [hosts];
  this.current_host = hosts[0];
  this.base_urls = _.map(hosts, function(h) {
    return this.options.protocol + '://' + h + ':' + port + prefix;
  });
  // 由于dataNode返回的是hostname,这里hack做个映射;
  // 这里记录一个问题：上传、下载文件时nameNode返回的是一个hostname，暂时解决桌面程序的主机做了/etc/hosts映射。
  // http://node1:50075/webhdfs/v1/test/test.txt?op=CREATE&namenoderpcaddress=ns&overwrite=false&replication=3
  var data_nodes = this.options.datanode_hosts || this.options.datanode_host; // {node1: '192.168.1.1'}
  if (!data_nodes) throw new Error("WebHdfsClient(...) 缺少 datanode_hosts:{} 配置!");
};

_.extend(WebHdfsClient.prototype, {

  /** 集成kerberos安全验证
   *
   * [addAuth description]
   * @method addAuth
   * @param  {[type]} cookies  [description] cookies
   */
  addAuth: function(cookies) {
    var cookie = request.cookie(cookies);
    // e.g. url: http://www.google.com
    this.jar.setCookie(cookie, this.options.protocol + '://' + this.current_host + ':' + this.options.namenode_port);
  },

  del: function*(path, hdfsoptions, requestoptions) {
    var args = this._makeReqArg('delete', path, hdfsoptions, requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('del', args)).boolean;
  },

  listStatus: function*(path, hdfsoptions, requestoptions) {
    var args = this._makeReqArg('liststatus', path, hdfsoptions, requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('get', args)).FileStatuses.FileStatus;
  },

  getFileStatus: function*(path, hdfsoptions, requestoptions) {
    var args = this._makeReqArg('getfilestatus', path, hdfsoptions, requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('get', args)).FileStatus;
  },

  getContentSummary: function*(path, hdfsoptions, requestoptions) {
    var args = this._makeReqArg('getcontentsummary', path, hdfsoptions, requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('get', args)).ContentSummary;
  },

  getFileChecksum: function*(path, hdfsoptions, requestoptions) {
    var args = this._makeReqArg('getfilechecksum', path, hdfsoptions, requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('get', args)).FileChecksum;
  },

  getHomeDirectory: function*(hdfsoptions, requestoptions) {
    var args = this._makeReqArg('gethomedirectory', '', hdfsoptions, requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('get', args)).Path;
  },

  rename: function*(path, destination, hdfsoptions, requestoptions) {
    var args = this._makeReqArg('rename', path, _.defaults(hdfsoptions || {}, {
      destination: destination
    }), requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('put', args)).boolean;
  },

  mkdirs: function*(path, hdfsoptions, requestoptions) {
    var args = this._makeReqArg('mkdirs', path, hdfsoptions, requestoptions);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    args = _.map(args, function(obj) {
      obj.uri = encodeURI(obj.uri);
      return obj;
    });
    console.log("hack!！uri处理后: %s", args[0].uri);

    return (yield * this._request('put', args)).Path;
  },

  open: function*(path, hdfsoptions, requestoptions) {
    return yield * this._open(path, hdfsoptions, requestoptions, false);
  },

  create: function*(path, data, hdfsoptions, requestoptions) {
    if (data === true) data = 'true';
    return yield * this._create(path, hdfsoptions, requestoptions, data);
  },

  append: function*(path, data, hdfsoptions, requestoptions) {
    if (data === true) data = 'true';
    return yield * this._append(path, hdfsoptions, requestoptions, data);
  },

  createReadStream: function*(path, hdfsoptions, requestoptions) {
    return yield * this._open(path, hdfsoptions, requestoptions, true);
  },

  createAppendStream: function*(path, hdfsoptions, requestoptions) {
    return yield * this._append(path, hdfsoptions, requestoptions, true);
  },

  createWriteStream: function*(path, hdfsoptions, requestoptions) {
    return yield * this._create(path, hdfsoptions, requestoptions, true);
  },

  // hack T_T!! 替换datanode节点hostname为ip
  _replaceHost: function(url) {
    var d_port = ':' + (this.options.datanode_port || 50075); // 50075
    var hostname = url.substring(url.indexOf("://") + "://".length, url.indexOf(d_port));
    var ip = this.options.datanode_hosts[hostname];
    var new_url = url.replace(hostname, ip);
    console.log("%s --> %s", url, new_url);
    return new_url;
  },

  _open: function*(path, hdfsoptions, requestoptions, streamMode) {
    try {
      yield * this.listStatus('/__a_not_exists_path____');
    } // Set active name nodes.
    catch (e) {}

    var args = this._makeReqArg('open', path, hdfsoptions, _.defaults(requestoptions || {}, {
      followRedirect: false
    }));
    args[0].uri = encodeURI(args[0].uri);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    console.log("hack!！uri处理后: %s", args[0].uri);
    var res = yield request.get(args[0]);

    if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
    if (res.statusCode !== 307) throw new Error('Status 307 expected');

    args = args[0];
    // args.uri = res.headers.location;
    args.uri = this._replaceHost(res.headers.location); // hack T_T
    delete args.qs;
    delete args.json;

    return streamMode ? rawRequest.get(args) : yield * this._request('get', [args]);
  },

  _append: function*(path, hdfsoptions, requestoptions, streamMode) {
    try {
      yield * this.listStatus('/__a_not_exists_path____');
    } // Set active name nodes.
    catch (e) {}

    var args = this._makeReqArg('append', path, hdfsoptions, _.defaults(requestoptions || {}, {
      followRedirect: false
    }));
    args[0].uri = encodeURI(args[0].uri);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    console.log("hack!！uri处理后: %s", args[0].uri);

    var res = yield request.post(args[0]);
    if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
    if (res.statusCode !== 307) throw new Error('Status 307 expected');

    args = args[0];
    // args.uri = res.headers.location;
    args.uri = this._replaceHost(res.headers.location); // hack T_T
    delete args.qs;
    delete args.json;

    if (streamMode === true) {
      return rawRequest.post(args);
    } else {
      args.body = streamMode;
      return yield * this._request('post', [args]);
    }
  },

  _create: function*(path, hdfsoptions, requestoptions, streamMode) {
    try {
      yield * this.listStatus('/__a_not_exists_path____');
    } // Set active name nodes.
    catch (e) {}

    var args = this._makeReqArg('create', path, hdfsoptions, _.defaults(requestoptions || {}, {
      followRedirect: false
    }));
    args[0].uri = encodeURI(args[0].uri);
    // 由于浏览器对get请求url中的特殊字符(包含中文)编码，这样大部分的后端程序解析url时默认会解码，程序此处不编码的话就会有问题。e.g. 找不到强行被解码的文件！
    console.log("hack!！uri处理后: %s", args[0].uri);

    var res = yield request.put(args[0]);
    if (res.body && res.body.RemoteException) throw new Error(res.body.RemoteException.message);
    if (res.statusCode !== 307) throw new Error('Status 307 expected');

    args = args[0];
    // args.uri = res.headers.location;
    args.uri = this._replaceHost(res.headers.location); // hack T_T
    delete args.qs;
    delete args.json;

    if (streamMode === true) {
      return rawRequest.put(args);
    } else {
      args.body = streamMode;
      return yield * this._request('put', [args]);
    }
  },

  _request: function*(op, args, expectStatus) {
    var lastErr;

    for (var i = 0; i < args.length; i++) {
      try {
        var res = yield request[op](args[i]);
      } catch (e) {
        lastErr = new Error(e);
        continue;
      }

      if (res.body && res.body.RemoteException) {
        if (res.body.RemoteException.exception !== strStandbyException) {
          this._setActive(i);
          throw new Error(res.body.RemoteException.message);
        }
      } else if (expectStatus && expectStatus !== res.statusCode) {
        throw new Error('WebHdfs: Status code ' + expectStatus + ' expected.');
      } else if (res.statusCode !== 200 && res.statusCode !== 201) {
        var err;
        try {
          err = JSON.parse(res.body).RemoteException.message;
        } catch (e) {
          err = new Error('WebHdfs: Invalid status code.');
        }

        throw err;
      } else {
        this._setActive(i);
        return res.body;
      }
    }

    throw lastErr || new Error('WebHdfs: All namenodes are standby.');
  },

  _setActive: function(i) {
    if (i > 0) {
      var u0 = this.base_urls[0];
      this.base_urls[0] = this.base_urls[i];
      this.current_host = this.base_urls[i]; // hack!!!
      this.base_urls[i] = u0;
    }
  },

  _makeReqArg: function(op, path, hdfsoptions, requestoptions) {
    var me = this;

    return _.map(me.base_urls, function(baseurl) {
      return _.defaults(requestoptions || {}, {
        json: true,
        uri: baseurl + path,
        jar: me.jar, // 添加cookie
        qs: _.defaults(hdfsoptions || {}, {
          op: op,
          // 'delegation': me.options.token, //Authentication using Hadoop delegation token when security is on.
          'user.name': me.options.user
        })
      });
    });
  }
});
