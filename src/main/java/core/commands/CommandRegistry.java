package core.commands;

import core.commands.string.*;
import core.commands.generic.*;
import core.commands.server.*;
import core.commands.connection.*;
import core.commands.geo.*;
import core.commands.replication.*;
import core.commands.hash.*;
import core.commands.list.*;
import core.commands.set.*;
import core.commands.zset.*;
import core.commands.hll.*;
import core.commands.json.*;
import core.commands.bloom.*;
import core.commands.tdigest.*;
import core.commands.pubsub.*;
import core.commands.transaction.*;
import core.commands.scripting.*;

import java.util.HashMap;
import java.util.Map;

public class CommandRegistry {
    private static final Map<String, Command> commands = new HashMap<>();

    static {
        // String
        register("GET", new GetCommand());
        register("SET", new SetCommand());
        register("SETNX", new SetNxCommand());
        register("INCR", new IncrCommand());
        register("DECR", new DecrCommand());
        register("INCRBY", new IncrByCommand());
        register("DECRBY", new DecrByCommand());
        register("GETSET", new GetSetCommand());
        register("STRLEN", new StrLenCommand());
        register("BITCOUNT", new BitCountCommand());
        register("BITOP", new BitOpCommand());
        register("BITPOS", new BitPosCommand());
        register("BITFIELD", new BitFieldCommand());
        register("BITFIELD_RO", new BitFieldRoCommand());
        register("SETBIT", new SetBitCommand());
        register("GETBIT", new GetBitCommand());
        register("MSET", new MSetCommand());
        register("MGET", new MGetCommand());
        register("APPEND", new AppendCommand());
        register("GETRANGE", new GetRangeCommand());
        register("SETRANGE", new SetRangeCommand());
        register("SETEX", new SetExCommand());
        register("PSETEX", new PSetExCommand());
        register("MSETNX", new MSetNxCommand());
        register("INCRBYFLOAT", new IncrByFloatCommand());
        register("GETDEL", new GetDelCommand());
        register("GETEX", new GetExCommand());
        register("COPY", new CopyCommand());
        register("MOVE", new MoveCommand());
        register("RENAMENX", new RenameNxCommand());
        register("LCS", new LcsCommand());

        // Hash
        register("HSET", new HSetCommand());
        register("HGET", new HGetCommand());
        register("HMSET", new HMSetCommand());
        register("HMGET", new HmgetCommand());
        register("HLEN", new HLenCommand());
        register("HKEYS", new HKeysCommand());
        register("HVALS", new HValsCommand());
        register("HEXISTS", new HExistsCommand());
        register("HGETALL", new HGetAllCommand());
        register("HDEL", new HDelCommand());
        register("HINCRBY", new HIncrByCommand());
        register("HSETNX", new HSetNxCommand());
        register("HINCRBYFLOAT", new HIncrByFloatCommand());
        register("HEXPIRE", new HExpireCommand());
        register("HTTL", new HTtlCommand());
        register("HSTRLEN", new HStrLenCommand());
        register("HRANDFIELD", new HRandFieldCommand());

        // List
        register("LLEN", new LLenCommand());
        register("LINDEX", new LIndexCommand());
        register("LREM", new LRemCommand());
        register("LPOP", new LPopCommand());
        register("RPOP", new RPopCommand());
        register("LPUSH", new LPushCommand());
        register("RPUSH", new RPushCommand());
        register("LTRIM", new LTrimCommand());
        register("LRANGE", new LRangeCommand());
        register("BLPOP", new BlPopCommand());
        register("BRPOP", new BrPopCommand());
        register("BRPOPLPUSH", new BrPopLPushCommand());
        register("BLMOVE", new BlMoveCommand());
        register("RPOPLPUSH", new RPopLPushCommand());
        register("LMOVE", new LMoveCommand());
        register("LPUSHX", new LPushXCommand());
        register("RPUSHX", new RPushXCommand());
        register("LSET", new LSetCommand());
        register("LPOS", new LPosCommand());
        register("LINSERT", new LInsertCommand());
        register("LMPOP", new LmPopCommand());
        register("BLMPOP", new BlmPopCommand());
        register("ZMPOP", new ZmPopCommand());
        register("BZMPOP", new BzmPopCommand());

        // Set
        register("SPOP", new SPopCommand());
        register("SRANDMEMBER", new SRandMemberCommand());
        register("SMOVE", new SMoveCommand());
        register("SADD", new SAddCommand());
        register("SMEMBERS", new SMembersCommand());
        register("SREM", new SRemCommand());
        register("SISMEMBER", new SIsMemberCommand());
        register("SMISMEMBER", new SMIsMemberCommand());
        register("SCARD", new SCardCommand());
        register("SINTER", new SInterCommand());
        register("SUNION", new SUnionCommand());
        register("SDIFF", new SDiffCommand());
        register("SINTERSTORE", new SInterStoreCommand());
        register("SUNIONSTORE", new SUnionStoreCommand());
        register("SDIFFSTORE", new SDiffStoreCommand());
        register("SINTERCARD", new SInterCardCommand());

        // ZSet
        register("ZREMRANGEBYSCORE", new ZRemRangeByScoreCommand());
        register("ZREMRANGEBYRANK", new ZRemRangeByRankCommand());
        register("ZREVRANGEBYLEX", new ZRevRangeByLexCommand());
        register("ZLEXCOUNT", new ZLexCountCommand());
        register("ZRANGEBYLEX", new ZRangeByLexCommand());
        register("ZADD", new ZAddCommand());
        register("ZRANGE", new ZRangeCommand());
        register("ZREVRANGE", new ZRevRangeCommand());
        register("ZRANK", new ZRankCommand());
        register("ZREM", new ZRemCommand());
        register("ZINCRBY", new ZIncrByCommand());
        register("ZCARD", new ZCardCommand());
        register("ZCOUNT", new ZCountCommand());
        register("ZSCORE", new ZScoreCommand());
        register("ZRANGEBYSCORE", new ZRangeByScoreCommand());
        register("ZREVRANGEBYSCORE", new ZRevRangeByScoreCommand());
        register("BZPOPMIN", new BzPopMinCommand());
        register("BZPOPMAX", new BzPopMaxCommand());
        register("ZPOPMIN", new ZPopMinCommand());
        register("ZPOPMAX", new ZPopMaxCommand());
        register("ZUNIONSTORE", new ZUnionStoreCommand());
        register("ZINTERSTORE", new ZInterStoreCommand());
        register("ZDIFF", new ZDiffCommand());
        register("ZDIFFSTORE", new ZDiffStoreCommand());
        register("ZINTER", new ZInterCommand());
        register("ZUNION", new ZUnionCommand());
        register("ZINTERCARD", new ZInterCardCommand());
        register("ZRANGESTORE", new ZRangeStoreCommand());
        register("ZREVRANK", new ZRevRankCommand());
        register("ZMSCORE", new ZMScoreCommand());
        register("ZRANDMEMBER", new ZRandMemberCommand());

        // Bloom
        register("BF.ADD", new BfAddCommand());
        register("BF.EXISTS", new BfExistsCommand());
        register("BF.MADD", new BfMAddCommand());
        register("BF.MEXISTS", new BfMExistsCommand());
        
        // T-Digest
        register("TD.ADD", new TdAddCommand());
        register("TD.QUANTILE", new TdQuantileCommand());
        register("TD.CDF", new TdCdfCommand());
        register("TD.INFO", new TdInfoCommand());

        // JSON
        register("JSON.SET", new JsonSetCommand());
        register("JSON.GET", new JsonGetCommand());
        register("JSON.DEL", new JsonDelCommand());
        register("JSON.FORGET", new JsonDelCommand()); // Alias
        register("JSON.TYPE", new JsonTypeCommand());
        
        // HLL
        register("PFADD", new PfAddCommand());
        register("PFCOUNT", new PfCountCommand());
        register("PFMERGE", new PfMergeCommand());
        
        // Time / Generic
        register("LATENCY", new LatencyCommand());
        register("WAIT", new WaitCommand());
        register("MIGRATE", new MigrateCommand());
        register("PEXPIRE", new PexpireCommand());
        register("PTTL", new PttlCommand());
        register("EXPIREAT", new ExpireAtCommand());
        register("PEXPIREAT", new PexpireAtCommand());
        register("EXPIRETIME", new ExpireTimeCommand());
        register("PEXPIRETIME", new PExpireTimeCommand());
        register("PERSIST", new PersistCommand());
        register("RANDOMKEY", new RandomKeyCommand());
        register("SORT", new SortCommand());
        register("SORT_RO", new SortRoCommand());
        register("EXISTS", new ExistsCommand());
        register("TYPE", new TypeCommand());
        register("RENAME", new RenameCommand());
        register("TTL", new TtlCommand());
        register("EXPIRE", new ExpireCommand());
        register("KEYS", new KeysCommand());
        register("DEL", new DelCommand());
        register("DUMP", new DumpCommand());
        register("RESTORE", new RestoreCommand());
        register("UNLINK", new UnlinkCommand());
        register("SCAN", new ScanCommand());
        register("HSCAN", new HScanCommand());
        register("SSCAN", new SScanCommand());
        register("ZSCAN", new ZScanCommand());
        register("TOUCH", new TouchCommand());

        // Server / Config
        register("TIME", new TimeCommand());
        register("CONFIG", new ConfigCommand());
        register("RESET", new ResetCommand());
        register("MEMORY", new MemoryCommand());
        register("SLOWLOG", new SlowlogCommand());
        register("INFO", new InfoCommand());
        register("DBSIZE", new DbSizeCommand());
        register("FLUSHALL", new FlushAllCommand());
        register("FLUSHDB", new FlushDbCommand());
        register("BGREWRITEAOF", new BgRewriteAofCommand());
        register("MONITOR", new MonitorCommand());
        register("SWAPDB", new SwapDbCommand());
        register("SAVE", new SaveCommand());
        register("BGSAVE", new BgSaveCommand());
        register("LASTSAVE", new LastSaveCommand());
        register("SHUTDOWN", new ShutdownCommand());
        register("ROLE", new RoleCommand());
        register("LOLWUT", new LolwutCommand());
        register("COMMAND", new CommandInfoCommand());
        register("OBJECT", new ObjectCommand());

        // Scripting
        register("EVAL", new EvalCommand());
        register("EVALSHA", new EvalShaCommand());
        register("EVAL_RO", new EvalRoCommand());
        register("EVALSHA_RO", new EvalShaRoCommand());
        register("SCRIPT", new ScriptCommand());

        // GEO
        register("GEOADD", new GeoAddCommand());
        register("GEODIST", new GeoDistCommand());
        register("GEORADIUS", new GeoRadiusCommand());
        register("GEORADIUSBYMEMBER", new GeoRadiusByMemberCommand());
        register("GEOHASH", new GeoHashCommand());
        register("GEOPOS", new GeoPosCommand());
        register("GEOSEARCH", new GeoSearchCommand());
        register("GEOSEARCHSTORE", new GeoSearchStoreCommand());
        
        // Connection
        register("ECHO", new EchoCommand());
        register("SELECT", new SelectCommand());
        register("PING", new PingCommand());
        register("QUIT", new QuitCommand());
        register("AUTH", new AuthCommand());

        // PubSub
        register("SUBSCRIBE", new SubscribeCommand());
        register("UNSUBSCRIBE", new UnsubscribeCommand());
        register("PSUBSCRIBE", new PSubscribeCommand());
        register("PUNSUBSCRIBE", new PUnsubscribeCommand());
        register("PUBLISH", new PublishCommand());
        register("PUBSUB", new PubSubCommand());

        // Transactions
        register("MULTI", new MultiCommand());
        register("EXEC", new ExecCommand());
        register("DISCARD", new DiscardCommand());
        register("WATCH", new WatchCommand());
        register("UNWATCH", new UnwatchCommand());

        // Replication
        register("REPLICAOF", new ReplicaOfCommand());
        register("SLAVEOF", new ReplicaOfCommand());
        register("PSYNC", new PsyncCommand());
        register("SYNC", new PsyncCommand());
        register("REPLCONF", new ReplconfCommand());
        
        // CLIENT command router
        register("CLIENT", new ClientCommand());
    }

    public static void register(String name, Command command) {
        commands.put(name, command);
    }

    public static Command getCommand(String name) {
        return commands.get(name);
    }
    
    public static Command get(String name) {
        return commands.get(name);
    }
}
