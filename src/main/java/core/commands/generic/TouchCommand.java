package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TouchCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: TOUCH key [key ...]");
            return;
        }
        
        int touched = 0;
        for (int i = 1; i < args.size(); i++) {
            String key = new String(args.get(i), StandardCharsets.UTF_8);
            // get() automatically calls touch()
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v != null) {
                touched++;
            }
        }
        client.sendInteger(touched);
    }
}
