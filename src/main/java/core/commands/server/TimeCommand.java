package core.commands.server;

import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class TimeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        long now = System.currentTimeMillis();
        long seconds = now / 1000;
        long micros = (now % 1000) * 1000;
        
        List<byte[]> response = new ArrayList<>();
        response.add(String.valueOf(seconds).getBytes(StandardCharsets.UTF_8));
        response.add(String.valueOf(micros).getBytes(StandardCharsets.UTF_8));
        
        client.sendResponse(Resp.array(response), null);
    }
}
