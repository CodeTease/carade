package core.commands.pubsub;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SubscribeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: SUBSCRIBE channel");
            return;
        }
        for (int i = 1; i < args.size(); i++) {
            String channel = new String(args.get(i), StandardCharsets.UTF_8);
            Carade.pubSub.subscribe(channel, client);
            client.isSubscribed = true;
            if (client.isResp()) {
                List<byte[]> resp = new ArrayList<>();
                resp.add("subscribe".getBytes(StandardCharsets.UTF_8));
                resp.add(channel.getBytes(StandardCharsets.UTF_8));
                resp.add("1".getBytes(StandardCharsets.UTF_8));
                client.send(true, Resp.array(resp), null);
            } else {
                client.send(false, null, "Subscribed to channel: " + channel);
            }
        }
    }
}
