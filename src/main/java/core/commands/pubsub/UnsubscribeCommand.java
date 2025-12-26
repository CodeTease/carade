package core.commands.pubsub;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class UnsubscribeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            // Unsubscribe all
            Carade.pubSub.unsubscribeAll(client);
            if (client.isSubscribed) {
                client.isSubscribed = false; 
                if (client.isResp()) {
                    List<byte[]> resp = new ArrayList<>();
                    resp.add("unsubscribe".getBytes(StandardCharsets.UTF_8));
                    resp.add(null);
                    resp.add("0".getBytes(StandardCharsets.UTF_8));
                    client.send(true, Resp.array(resp), null);
                }
                else client.send(false, null, "Unsubscribed from all");
            }
        } else {
            for (int i = 1; i < args.size(); i++) {
                String channel = new String(args.get(i), StandardCharsets.UTF_8);
                Carade.pubSub.unsubscribe(channel, client);
                if (client.isResp()) {
                    List<byte[]> resp = new ArrayList<>();
                    resp.add("unsubscribe".getBytes(StandardCharsets.UTF_8));
                    resp.add(channel.getBytes(StandardCharsets.UTF_8));
                    resp.add("0".getBytes(StandardCharsets.UTF_8));
                    client.send(true, Resp.array(resp), null);
                }
                else client.send(false, null, "Unsubscribed from: " + channel);
            }
        }
    }
}
