package core.commands.pubsub;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PUnsubscribeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() >= 2) {
            for (int i = 1; i < args.size(); i++) {
                String pattern = new String(args.get(i), StandardCharsets.UTF_8);
                Carade.pubSub.punsubscribe(pattern, client);
                if (client.isResp()) {
                    List<byte[]> resp = new ArrayList<>();
                    resp.add("punsubscribe".getBytes(StandardCharsets.UTF_8));
                    resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                    resp.add("0".getBytes(StandardCharsets.UTF_8));
                    client.send(true, Resp.array(resp), null);
                } 
                else client.send(false, null, "Unsubscribed from pattern: " + pattern);
            }
        }
    }
}
