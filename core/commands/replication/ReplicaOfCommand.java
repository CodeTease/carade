package core.commands.replication;

import core.commands.Command;
import core.network.ClientHandler;
import core.replication.ReplicationManager;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReplicaOfCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'replicaof' command");
            return;
        }

        String host = new String(args.get(1), StandardCharsets.UTF_8);
        String portStr = new String(args.get(2), StandardCharsets.UTF_8);

        if (host.equalsIgnoreCase("NO") && portStr.equalsIgnoreCase("ONE")) {
            ReplicationManager.getInstance().slaveOf(null, -1);
            client.sendResponse(Resp.simpleString("OK"), "OK");
            return;
        }

        try {
            int port = Integer.parseInt(portStr);
            ReplicationManager.getInstance().slaveOf(host, port);
            client.sendResponse(Resp.simpleString("OK"), "OK");
        } catch (NumberFormatException e) {
            client.sendError("ERR port is not an integer");
        }
    }
}
