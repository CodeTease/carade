package core.commands;

import core.commands.string.SetCommand;
import core.commands.string.SetNxCommand;
import core.commands.generic.*;
import core.commands.server.ConfigGetCommand;
import core.commands.connection.*;
import java.util.HashMap;
import java.util.Map;
import core.network.ClientHandler;
import java.util.List;

public class CommandRegistry {
    private static final Map<String, Command> commands = new HashMap<>();

    static {
        register("SET", new SetCommand());
        register("SETNX", new SetNxCommand());
        
        // Time / Generic
        register("PEXPIRE", new PexpireCommand());
        register("PTTL", new PttlCommand());
        register("EXPIREAT", new ExpireAtCommand());
        register("PEXPIREAT", new PexpireAtCommand());
        register("PERSIST", new PersistCommand());
        
        // Server / Config
        register("CONFIG", new ConfigGetCommand());
        
        // Connection
        register("SELECT", new SelectCommand());
        
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
