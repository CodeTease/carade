package core.commands.replication;

import core.commands.Command;
import core.network.ClientHandler;
import core.replication.ReplicationManager;
import java.util.List;

public class WaitCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'wait' command");
            return;
        }
        
        try {
            int currentReplicas = ReplicationManager.getInstance().getConnectedReplicasCount();
            client.sendInteger(currentReplicas);

        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
