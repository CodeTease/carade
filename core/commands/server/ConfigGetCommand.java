package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ConfigGetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // args[0] = CONFIG, args[1] = GET, args[2] = pattern
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

        client.sendResponse(Resp.array(result), null);
    }
}
