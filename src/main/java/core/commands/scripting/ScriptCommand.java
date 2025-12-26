package core.commands.scripting;

import core.commands.Command;
import core.network.ClientHandler;
import core.scripting.ScriptManager;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ScriptCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'script' command");
            return;
        }

        String subcommand = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();

        switch (subcommand) {
            case "LOAD":
                if (args.size() < 3) {
                    client.sendError("ERR wrong number of arguments for 'script|load' command");
                    return;
                }
                String script = new String(args.get(2), StandardCharsets.UTF_8);
                String sha = ScriptManager.getInstance().load(script);
                client.sendBulkString(sha);
                break;
                
            case "EXISTS":
                if (args.size() < 3) {
                    client.sendError("ERR wrong number of arguments for 'script|exists' command");
                    return;
                }
                List<Object> results = new java.util.ArrayList<>();
                for (int i = 2; i < args.size(); i++) {
                    String s = new String(args.get(i), StandardCharsets.UTF_8);
                    boolean exists = ScriptManager.getInstance().exists(s);
                    results.add(exists ? 1L : 0L);
                }
                client.sendMixedArray(results);
                break;
                
            case "FLUSH":
                ScriptManager.getInstance().flush();
                client.sendSimpleString("OK");
                break;

            case "KILL":
                try {
                    ScriptManager.getInstance().killScript();
                    client.sendSimpleString("OK");
                } catch (Exception e) {
                    client.sendError("ERR " + e.getMessage());
                }
                break;
                
            default:
                client.sendError("ERR Unknown subcommand or wrong number of arguments for '" + subcommand + "'. Try SCRIPT HELP.");
        }
    }
}
