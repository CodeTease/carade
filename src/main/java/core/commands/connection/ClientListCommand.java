package core.commands.connection;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class ClientListCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        StringBuilder sb = new StringBuilder();
        for (ClientHandler c : Carade.connectedClients) {
            sb.append("id=").append(System.identityHashCode(c))
              .append(" addr=").append(c.getRemoteAddress())
              .append(" name=").append(c.getClientName() == null ? "" : c.getClientName())
              .append(" db=").append(c.getDbIndex())
              .append("\n");
        }
        client.sendBulkString(sb.toString());
    }
}
