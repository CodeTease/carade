package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ConfigCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'config' command");
            return;
        }

        String sub = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();

        switch (sub) {
            case "GET":
                handleGet(client, args);
                break;
            case "SET":
                handleSet(client, args);
                break;
            case "RESETSTAT":
                handleResetStat(client);
                break;
            default:
                client.sendError("ERR unknown subcommand for 'config'");
        }
    }

    private void handleGet(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: CONFIG GET parameter");
            return;
        }

        String pattern = new String(args.get(2), StandardCharsets.UTF_8);
        List<byte[]> result = new ArrayList<>();

        if (pattern.equals("*") || pattern.equalsIgnoreCase("port")) {
            result.add("port".getBytes(StandardCharsets.UTF_8));
            result.add(String.valueOf(Carade.config.port).getBytes(StandardCharsets.UTF_8));
        }
        if (pattern.equals("*") || pattern.equalsIgnoreCase("maxmemory")) {
            result.add("maxmemory".getBytes(StandardCharsets.UTF_8));
            result.add(String.valueOf(Carade.config.maxMemory).getBytes(StandardCharsets.UTF_8));
        }
        if (pattern.equals("*") || pattern.equalsIgnoreCase("maxmemory-policy")) {
            result.add("maxmemory-policy".getBytes(StandardCharsets.UTF_8));
            result.add(Carade.config.maxMemoryPolicy.getBytes(StandardCharsets.UTF_8));
        }
        // Add more as needed

        client.sendResponse(Resp.array(result), null);
    }

    private void handleSet(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
             client.sendError("usage: CONFIG SET parameter value");
             return;
        }
        
        String param = new String(args.get(2), StandardCharsets.UTF_8).toLowerCase();
        String value = new String(args.get(3), StandardCharsets.UTF_8);
        
        try {
            switch (param) {
                case "maxmemory":
                    // Parse memory format logic duplicated from Config, 
                    // or just assume bytes for runtime SET? Redis usually allows 10MB etc.
                    // For simplicity, let's assume raw integer bytes for now or copy parsing logic.
                    // Let's do simple long parse.
                    Carade.config.maxMemory = Long.parseLong(value); 
                    break;
                case "maxmemory-policy":
                    Carade.config.maxMemoryPolicy = value;
                    break;
                case "requirepass":
                    Carade.config.password = value;
                    // Also update default user?
                    if (Carade.config.users.containsKey("default")) {
                        Carade.config.users.get("default").password = value;
                    }
                    break;
                default:
                    client.sendError("ERR Unsupported CONFIG parameter: " + param);
                    return;
            }
            client.sendSimpleString("OK");
        } catch (NumberFormatException e) {
            client.sendError("ERR invalid value");
        }
    }

    private void handleResetStat(ClientHandler client) {
        Carade.totalCommands.set(0);
        Carade.keyspaceHits.set(0);
        Carade.keyspaceMisses.set(0);
        Carade.slowLog.clear();
        client.sendSimpleString("OK");
    }
}
