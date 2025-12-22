package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.io.IOException;
import java.util.List;

public class MonitorCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> parts) {
        client.setMonitor(true);
        Carade.monitors.add(client);
        client.send(true, Resp.simpleString("OK"), "OK");
    }
}
