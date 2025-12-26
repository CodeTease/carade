package core.commands.pubsub;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PSubscribeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: PSUBSCRIBE pattern");
            return;
        }
        for (int i = 1; i < args.size(); i++) {
            String pattern = new String(args.get(i), StandardCharsets.UTF_8);
            Carade.pubSub.psubscribe(pattern, client);
            client.isSubscribed = true;
            if (client.isResp()) {
                List<byte[]> resp = new ArrayList<>();
                resp.add("psubscribe".getBytes(StandardCharsets.UTF_8));
                resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                resp.add("1".getBytes(StandardCharsets.UTF_8));
                client.send(true, Resp.array(resp), null);
            } else {
                client.send(false, null, "Subscribed to pattern: " + pattern);
            }
        }
    }
}
