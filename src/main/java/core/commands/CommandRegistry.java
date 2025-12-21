package core.commands;

import core.commands.string.SetCommand;
import core.commands.string.SetNxCommand;
import core.commands.string.IncrByCommand;
import core.commands.string.DecrByCommand;
import core.commands.string.BitCountCommand;
import core.commands.string.BitOpCommand;
import core.commands.generic.*;
import core.commands.server.ConfigGetCommand;
import core.commands.server.SlowlogCommand;
import core.commands.connection.*;
import core.commands.geo.*;
import core.commands.replication.*;
import core.commands.hash.*;
import core.commands.list.*;
import core.commands.set.*;
import core.commands.zset.*;
import core.commands.hll.*;
import core.commands.string.StrLenCommand;
import core.commands.string.GetSetCommand;
import java.util.HashMap;
import java.util.Map;
import core.network.ClientHandler;
import java.util.List;

public class CommandRegistry {
    private static final Map<String, Command> commands = new HashMap<>();

    static {
        register("SET", new SetCommand());
        register("SETNX", new SetNxCommand());
        register("INCRBY", new IncrByCommand());
        register("DECRBY", new DecrByCommand());
        register("GETSET", new GetSetCommand());
        register("STRLEN", new StrLenCommand());
        register("BITCOUNT", new BitCountCommand());
        register("BITOP", new BitOpCommand());

        // Hash
        register("HMGET", new HmgetCommand());
        register("HLEN", new HLenCommand());
        register("HKEYS", new HKeysCommand());
        register("HVALS", new HValsCommand());
        register("HEXISTS", new HExistsCommand());

        // List
        register("LLEN", new LLenCommand());
        register("LINDEX", new LIndexCommand());
        register("LREM", new LRemCommand());

        // Set
        register("SPOP", new SPopCommand());
        register("SRANDMEMBER", new SRandMemberCommand());

        // ZSet
        register("ZREMRANGEBYSCORE", new ZRemRangeByScoreCommand());
        register("ZREMRANGEBYRANK", new ZRemRangeByRankCommand());
        
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
        
        // Server / Config
        register("CONFIG", new ConfigGetCommand());
        register("SLOWLOG", new SlowlogCommand());

        // GEO
        register("GEOADD", new GeoAddCommand());
        register("GEODIST", new GeoDistCommand());
        register("GEORADIUS", new GeoRadiusCommand());
        
        // Connection
        register("SELECT", new SelectCommand());

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