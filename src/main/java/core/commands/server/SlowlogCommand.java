package core.commands.server;

import core.commands.Command;
import core.network.ClientHandler;
import core.Carade;
import core.protocol.Resp;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class SlowlogCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
             client.sendError("ERR wrong number of arguments for 'slowlog' command");
             return;
        }
        
        String sub = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();
        if (sub.equals("LEN")) {
            client.sendInteger(Carade.slowLog.size());
        } else if (sub.equals("RESET")) {
            Carade.slowLog.clear();
            client.send(client.isResp(), Resp.simpleString("OK"), "OK");
        } else if (sub.equals("GET")) {
            int count = 10;
            if (args.size() >= 3) {
                try {
                    count = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
                } catch (NumberFormatException e) {
                     client.sendError("ERR value is not an integer or out of range");
                     return;
                }
            }
            
            // Return recent logs (tail of deque)
            // Deque iterates from first (oldest) to last (newest).
            // Usually slowlog get 10 means get *latest* 10.
            // So we should iterate descending.
            
            List<String> logs = new ArrayList<>(Carade.slowLog);
            List<byte[]> result = new ArrayList<>();
            
            int found = 0;
            for (int i = logs.size() - 1; i >= 0 && found < count; i--) {
                result.add(logs.get(i).getBytes(StandardCharsets.UTF_8));
                found++;
            }
            
            client.sendArray(result);
        } else {
             client.sendError("ERR unknown subcommand for 'slowlog'");
        }
    }
}
