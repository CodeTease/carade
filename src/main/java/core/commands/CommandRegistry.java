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
import core.commands.pubsub.*;
import core.commands.transaction.*;

import java.util.HashMap;
import java.util.Map;
import core.network.ClientHandler;
import java.util.List;

public class CommandRegistry {
    private static final Map<String, Command> commands = new HashMap<>();

    static {
        // String
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
        register("SETBIT", new SetBitCommand());
        register("GETBIT", new GetBitCommand());
        register("MSET", new MSetCommand());
        register("MGET", new MGetCommand());
        register("APPEND", new AppendCommand());
        register("GETRANGE", new GetRangeCommand());
        register("SETRANGE", new SetRangeCommand());

        // Hash
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

        // List
        register("LLEN", new LLenCommand());
        register("LINDEX", new LIndexCommand());
        register("LREM", new LRemCommand());
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

        // Set
        register("SPOP", new SPopCommand());
        register("SRANDMEMBER", new SRandMemberCommand());
        register("SADD", new SAddCommand());
        register("SMEMBERS", new SMembersCommand());
        register("SREM", new SRemCommand());
        register("SISMEMBER", new SIsMemberCommand());
        register("SCARD", new SCardCommand());
        register("SINTER", new SInterCommand());
        register("SUNION", new SUnionCommand());
        register("SDIFF", new SDiffCommand());
        register("SINTERSTORE", new SInterStoreCommand());
        register("SUNIONSTORE", new SUnionStoreCommand());
        register("SDIFFSTORE", new SDiffStoreCommand());

        // ZSet
        register("ZREMRANGEBYSCORE", new ZRemRangeByScoreCommand());
        register("ZREMRANGEBYRANK", new ZRemRangeByRankCommand());
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
        register("ZREVRANGEBYSCORE", new ZRangeByScoreCommand()); // Reuse same class
        register("ZPOPMIN", new ZPopMinCommand());
        register("ZPOPMAX", new ZPopMaxCommand());
        register("ZUNIONSTORE", new ZUnionStoreCommand());
        register("ZINTERSTORE", new ZInterStoreCommand());

        // Bloom
        register("BF.ADD", new BfAddCommand());
        register("BF.EXISTS", new BfExistsCommand());
        
        // JSON
        register("JSON.SET", new JsonSetCommand());
        register("JSON.GET", new JsonGetCommand());
        
        // HLL
        register("PFADD", new PfAddCommand());
        register("PFCOUNT", new PfCountCommand());
        
        // Time / Generic
        register("PEXPIRE", new PexpireCommand());
        register("PTTL", new PttlCommand());
        register("EXPIREAT", new ExpireAtCommand());
        register("PEXPIREAT", new PexpireAtCommand());
        register("PERSIST", new PersistCommand());
        register("RANDOMKEY", new RandomKeyCommand());
        register("SORT", new SortCommand());
        register("EXISTS", new ExistsCommand());
        register("TYPE", new TypeCommand());
        register("RENAME", new RenameCommand());
        register("TTL", new TtlCommand());
        register("EXPIRE", new ExpireCommand());
        register("KEYS", new KeysCommand());
        register("UNLINK", new UnlinkCommand());
        register("SCAN", new ScanCommand());
        register("HSCAN", new ScanCommand());
        register("SSCAN", new ScanCommand());
        register("ZSCAN", new ScanCommand());

        // Server / Config
        register("CONFIG", new ConfigGetCommand());
        register("SLOWLOG", new SlowlogCommand());
        register("INFO", new InfoCommand());
        register("DBSIZE", new DbSizeCommand());
        register("FLUSHALL", new FlushAllCommand());
        register("FLUSHDB", new FlushDbCommand());
        register("BGREWRITEAOF", new BgRewriteAofCommand());

        // GEO
        register("GEOADD", new GeoAddCommand());
        register("GEODIST", new GeoDistCommand());
        register("GEORADIUS", new GeoRadiusCommand());
        
        // Connection
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
        register("CLIENT", new Command() {
            @Override
            public void execute(ClientHandler client, List<byte[]> args) {
                if (args.size() < 2) {
                    client.sendError("ERR wrong number of arguments for 'client' command");
                    return;
                }
                String sub = new String(args.get(1)).toUpperCase();
                if (sub.equals("SETNAME")) {
                    new ClientSetNameCommand().execute(client, args);
                } else if (sub.equals("GETNAME")) {
                    new ClientGetNameCommand().execute(client, args);
                } else {
                    client.sendError("ERR unknown subcommand for 'client'");
                }
            }
        });
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
