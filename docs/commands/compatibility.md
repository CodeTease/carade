# Compatibility Matrix

Carade aims to be highly compatible with Redis commands. Below is the list of supported commands as of version 0.3.4.

## Supported Commands

### String
*   `GET`, `SET`, `SETNX`, `GETSET`
*   `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`
*   `MSET`, `MGET`, `MSETNX`
*   `APPEND`, `GETRANGE`, `SETRANGE`, `STRLEN`
*   `SETEX`, `PSETEX`, `GETEX`, `GETDEL`
*   `BITCOUNT`, `BITOP`, `BITPOS`, `BITFIELD`, `BITFIELD_RO`, `SETBIT`, `GETBIT`
*   `LCS`

### List
*   `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`
*   `LPOP`, `RPOP`, `BLPOP`, `BRPOP`
*   `LLEN`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LRANGE`
*   `RPOPLPUSH`, `BRPOPLPUSH`, `LMOVE`, `BLMOVE`
*   `LPOS`, `LMPOP`, `BLMPOP`

### Hash
*   `HSET`, `HGET`, `HMSET`, `HMGET`, `HGETALL`
*   `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`
*   `HINCRBY`, `HINCRBYFLOAT`
*   `HSETNX`, `HSTRLEN`, `HRANDFIELD`
*   `HSCAN`, `HEXPIRE`, `HTTL`

### Set
*   `SADD`, `SREM`, `SPOP`, `SMOVE`
*   `SCARD`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SRANDMEMBER`
*   `SINTER`, `SUNION`, `SDIFF`
*   `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`
*   `SINTERCARD`, `SSCAN`

### Sorted Set (ZSet)
*   `ZADD`, `ZREM`, `ZINCRBY`
*   `ZCARD`, `ZCOUNT`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`
*   `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`
*   `ZPOPMIN`, `ZPOPMAX`, `BZPOPMIN`, `BZPOPMAX`, `ZMPOP`, `BZMPOP`
*   `ZUNION`, `ZINTER`, `ZDIFF`
*   `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`
*   `ZINTERCARD`, `ZRANDMEMBER`, `ZSCAN`

### Geo
*   `GEOADD`, `GEODIST`, `GEOHASH`, `GEOPOS`
*   `GEORADIUS`, `GEORADIUSBYMEMBER`
*   `GEOSEARCH`, `GEOSEARCHSTORE`

### Pub/Sub
*   `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`
*   `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBSUB`

### Transactions
*   `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

### Scripting
*   `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT`

### JSON (Carade Extension)
*   `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE`, `JSON.FORGET`

### Probabilistic
*   **HyperLogLog:** `PFADD`, `PFCOUNT`, `PFMERGE`
*   **Bloom Filter:** `BF.ADD`, `BF.EXISTS`, `BF.MADD`, `BF.MEXISTS`
*   **T-Digest:** `TD.ADD`, `TD.QUANTILE`, `TD.CDF`, `TD.INFO`

### Server / Connection
*   `PING`, `ECHO`, `SELECT`, `QUIT`, `AUTH`
*   `INFO`, `DBSIZE`, `TIME`, `CONFIG`, `COMMAND`, `Lolwut`
*   `FLUSHDB`, `FLUSHALL`
*   `SAVE`, `BGSAVE`, `LASTSAVE`, `BGREWRITEAOF`
*   `SHUTDOWN`, `ROLE`, `REPLICAOF`, `SLAVEOF`
*   `PSYNC`, `SYNC`, `REPLCONF`
*   `CLIENT`, `MONITOR`, `SLOWLOG`, `LATENCY`
*   `MEMORY`, `SWAPDB`, `WAIT`

### Keys / Generic
*   `DEL`, `UNLINK`, `EXISTS`, `TYPE`
*   `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `EXPIRETIME`, `PEXPIRETIME`
*   `TTL`, `PTTL`, `PERSIST`, `TOUCH`
*   `KEYS`, `SCAN`, `RANDOMKEY`, `RENAME`, `RENAMENX`
*   `DUMP`, `RESTORE`, `COPY`, `MOVE`
*   `SORT`, `SORT_RO`

## Unsupported / Missing

*   **Cluster:** Not supported.
*   **Streams:** (`XADD`, `XREAD`, etc.) Not implemented.
*   **Modules:** No dynamic module loading.
*   **ACL:** Full ACL commands (`ACL SETUSER`, etc.) are missing; use `carade.conf`.
