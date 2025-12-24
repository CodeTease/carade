package core.commands.server;

import core.commands.Command;
import core.network.ClientHandler;
import core.replication.ReplicationManager;
import java.util.List;
import core.protocol.Resp;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

public class RoleCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        ReplicationManager repl = ReplicationManager.getInstance();
        List<Object> result = new ArrayList<>();
        
        if (repl.isSlave()) {
            result.add("slave");
            result.add(repl.getMasterHost());
            result.add(repl.getMasterPort());
            result.add("connected"); // Simplified state
            result.add(repl.getReplicationOffset());
        } else {
            result.add("master");
            result.add(repl.getReplicationOffset());
            List<Object> slaves = new ArrayList<>();
            // We would need to iterate replicas to list them
            // ReplicationManager doesn't expose replicas list easily public.
            // But we can just return empty list for now as we don't track detailed slave info publicly in ReplicationManager yet.
            result.add(slaves); 
        }
        
        client.sendMixedArray(result);
    }
}
