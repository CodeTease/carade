package core.commands.scripting;

import core.commands.Command;
import core.network.ClientHandler;
import core.scripting.ScriptManager;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EvalCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        System.out.println("DEBUG: EvalCommand executed");
        // EVAL script numkeys key [key ...] arg [arg ...]
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'eval' command");
            return;
        }

        String script = new String(args.get(1), StandardCharsets.UTF_8);
        
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        if (numKeys < 0) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        if (args.size() < 3 + numKeys) {
            client.sendError("ERR wrong number of arguments for 'eval' command");
            return;
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            keys.add(new String(args.get(3 + i), StandardCharsets.UTF_8));
        }

        List<String> scriptArgs = new ArrayList<>();
        for (int i = 3 + numKeys; i < args.size(); i++) {
            scriptArgs.add(new String(args.get(i), StandardCharsets.UTF_8));
        }

        try {
            System.out.println("DEBUG: Calling ScriptManager.eval");
            Object result = ScriptManager.getInstance().eval(client, script, keys, scriptArgs, false);
            System.out.println("DEBUG: ScriptManager.eval returned " + result);
            if (result == null) {
                client.sendNull();
            } else if (result instanceof Long) {
                client.sendInteger((Long) result);
            } else if (result instanceof byte[]) {
                client.send(true, Resp.bulkString((byte[]) result), null);
            } else if (result instanceof String) { 
                client.sendSimpleString((String) result); 
            } else if (result instanceof List) {
                client.sendMixedArray((List<Object>) result);
            } else if (result instanceof core.scripting.RespParser.RespError) {
                client.sendError(((core.scripting.RespParser.RespError) result).message);
            } else {
                client.sendError("ERR Unknown result type from script");
            }
        } catch (Throwable e) {
            System.out.println("DEBUG: Error in EvalCommand: " + e);
            e.printStackTrace();
            client.sendError(e.getMessage());
        }
    }
}
