#co-webhdfs-plus
  A webhdfs client in [co](https://github.com/tj/co) style.

##NOTE:
  - Node verion should >= 0.11.0
  - For more information about co, take a look at [document for co](https://github.com/visionmedia/co)
  - For more information about webhdfs, take a look at [document for webhdfs](http://hadoop.apache.org/docs/stable2/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)

##WHY
> 其实就是`hdfs restful client`读写文件时，webHDFS会307重定向一次，但是此时返回的是datanode域名，由于业务需要
> 我把域名转换成了事先对应的ip地址。

> e.g.

> [http://node1:50075/webhdfs/v1/test/test.txt?op=CREATE&namenoderpcaddress=ns&overwrite=false&replication=3]()

> 转换成

> [http://192.168.1.10:50075/webhdfs/v1/test/test.txt?op=CREATE&namenoderpcaddress=ns&overwrite=false&replication=3]()

##Install:
```
npm install co-webhdfs-plus
```

##Usage:
```js
var _ = require('codash'),
    co = require('co'),
    LineReader = require('co-stream').LineReader,
    Writer = require('co-stream').Writer,
    WebHdfsClient = require('co-webhdfs');

co(function *() {
    var client = new WebHdfsClient({ namenode_hosts: ['10.10.0.141', '10.10.0.140'] });
    // Or, if you don't want to enable failover:
    // var client = new WebHdfsClient({ namenode_host: '10.10.0.140' });

    var home = yield* client.getHomeDirectory();
    var files = yield* client.listStatus(home);
    console.log(_.map(files, function (f) { return f.pathSuffix }));

    yield* client.mkdirs(home + '/tmp');

    var filename = home + '/tmp/foo.txt';

    // Data style.
    yield* client.del(filename);
    yield* client.create(filename, 'foooooo\n', { overwrite: true });
    yield* client.append(filename, 'barr\n');
    console.log(yield* client.open(filename));

    // Stream style.
    var stream = new Writer(yield* client.createWriteStream(filename, { overwrite: true }));
    yield stream.writeline('xxxxxxx');
    yield _.sleep(100);
    yield stream.writeline('gggggg');
    yield stream.end();
    yield _.sleep(1000);

    var reader = new LineReader(yield* client.createReadStream(filename));
    for (var line; line = yield* reader.read();) {
        console.log('line: ', line);
    }
}).then(function () {
    console.log('finished...');
}, function (err) {
    console.log(err);
    console.log(err.stack);
});
```

##kerberos SPNEGO
> Authentication using Hadoop delegation token when security is on:

```js
// e.g. http://<HOST>:<PORT>/webhdfs/v1/<PATH>?delegation=<TOKEN>&op=...
var client = new WebHdfsClient({ token: 'kerberos_token', ... });
```

or

```js
var client = new WebHdfsClient({ ... });
client.addAuth("cookies");
...
// cookies访问是有时效性的，所以client后续操作返回401响应，则让用户重新登录获取cookies
```


##API
##### Constructor
```js
WebHdfsClient(options)
  options:
    user: user name, default 'hadoop'
    namenode_host: ip/hotname of namenode, default 'localhost'
    namenode_port: port of webhdfa, default 50070
```
##### Create and Write to a File
```js
create: function *(path, data, hdfsoptions)
createWriteStream: function *(path, hdfsoptions)
  path: path of file
  data: data to write
  hdfsoptions:
    overwrite: <true|false>
    replication: <SHORT>]
    blocksize: <LONG>
    permission: <OCTAL>
    buffersize: <INT>
```
##### Append to a File
```js
append: function *(path, data, hdfsoptions)
createAppendStream: function *(path, hdfsoptions)
  path: path of file
  data: data to write
  hdfsoptions:
    buffersize: <INT>
```
##### Open and Read a File
```js
open: function *(path, hdfsoptions)
createReadStream: function *(path, hdfsoptions)
  path: path of file
  hdfsoptions:
    buffersize: <INT>
    offset:<LONG>
    length:<LONG>
```
##### Make a Directory
```js
mkdirs: function *(path, hdfsoptions)
  path: path of file
  hdfsoptions:
    permission: <OCTAL>
```
##### Rename a File/Directory
```js
rename: function *(from, to)
```
##### Delete a File/Directory
```js
del: function *(path)
```
##### Status of a File/Directory
```js
getFileStatus: function *(path)
```
##### List a Directory
```js
listStatus: function *(path)
```
##### Get Content Summary of a Directory
```js
getContentSummary: function *(path)
```
##### Get File Checksum
```js
getFileChecksum: function *(path)
```
##### Get Home Directory
```js
getHomeDirectory: function *()
```
