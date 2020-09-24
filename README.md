Report data provider 1.5

This component serves as a backend for report-viewer.

Report data provider requires two configuration files to start.
Configuration directory can be specified with an argument `-c "config_dir"`

Example of config files:

`config_dir/cradle.json`
```
{
  "host": "host",
  "port": 9042,
  "dataCenter": "datacenter",
  "keyspace": "keyspace",
  "username": "username",
  "password": "password"
}
```

`config_dir/custom.json`
```
{
  "hostname": "localhost",
  "port": 8888,

  "responseTimeout": 60000,
  "serverCacheTimeout": 60000,
  "clientCacheTimeout": 60,
  "eventCacheSize": 100000,
  "messageCacheSize": 100000,
  "ioDispatcherThreadPoolSize": 1,
  "cassandraInstance": "instance1",
  "cassandraQueryTimeout": 30000
}
```
