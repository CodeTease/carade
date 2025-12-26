package core.commands.server;

import core.commands.Command;
import core.commands.CommandContainer;
import core.commands.CommandMetadata;
import core.commands.CommandRegistry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CommandInfoCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // args[0] is "COMMAND"
        String sub = "";
        if (args.size() > 1) {
            sub = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();
        }

        if (sub.equals("COUNT")) {
            client.sendInteger(CommandRegistry.getAll().size());
        } else if (sub.equals("DOCS")) {
             // Not implemented yet
            client.sendMixedArray(new ArrayList<>());
        } else if (sub.equals("INFO")) {
             // COMMAND INFO [cmd ...]
             List<String> requestedCommands = new ArrayList<>();
             for (int i = 2; i < args.size(); i++) {
                 requestedCommands.add(new String(args.get(i), StandardCharsets.UTF_8).toUpperCase());
             }
             
             List<Object> reply = new ArrayList<>();
             for (String cmdName : requestedCommands) {
                 CommandContainer container = CommandRegistry.getAll().get(cmdName);
                 if (container != null) {
                     reply.add(formatCommandInfo(cmdName, container.getMetadata()));
                 } else {
                     reply.add(null); // Redis returns null (nil) for unknown commands in INFO
                 }
             }
             client.sendMixedArray(reply);
        } else {
            // COMMAND (list all)
            Map<String, CommandContainer> all = CommandRegistry.getAll();
            List<Object> reply = new ArrayList<>(all.size());
            // Redis COMMAND returns array of arrays. Order is random (hash map) usually ok.
            for (Map.Entry<String, CommandContainer> entry : all.entrySet()) {
                reply.add(formatCommandInfo(entry.getKey(), entry.getValue().getMetadata()));
            }
            client.sendMixedArray(reply);
        }
    }

    private List<Object> formatCommandInfo(String name, CommandMetadata meta) {
        List<Object> info = new ArrayList<>();
        info.add(name.toLowerCase().getBytes(StandardCharsets.UTF_8)); // Name
        info.add(meta.getArity()); // Arity
        
        // Flags
        List<byte[]> flags = new ArrayList<>();
        for (String flag : meta.getFlags()) {
            flags.add(flag.getBytes(StandardCharsets.UTF_8));
        }
        info.add(flags); 
        
        info.add(meta.getFirstKey()); // First key
        info.add(meta.getLastKey()); // Last key
        info.add(meta.getStep()); // Step
        
        // ACL (Redis 6+) - empty list for now as we don't have ACLs defined in metadata
        // info.add(Collections.emptyList()); 
        
        return info;
    }
}
