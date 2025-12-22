package core.commands.pubsub;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PublishCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: PUBLISH channel message");
            return;
        }
        String channel = new String(args.get(1), StandardCharsets.UTF_8);
        String msg = new String(args.get(2), StandardCharsets.UTF_8);
        int count = Carade.pubSub.publish(channel, msg);
        client.sendInteger(count);
    }
}
