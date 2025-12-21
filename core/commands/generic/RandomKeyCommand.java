package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class RandomKeyCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        Set<String> keys = Carade.db.keySet(client.dbIndex);
        if (keys.isEmpty()) {
            client.sendNull();
            return;
        }

        int size = keys.size();
        int index = ThreadLocalRandom.current().nextInt(size);
        
        String key = null;
        java.util.Iterator<String> it = keys.iterator();
        for (int i = 0; i <= index && it.hasNext(); i++) {
            key = it.next();
        }
        
        if (key != null) {
            client.sendBulkString(key);
        } else {
            client.sendNull();
        }
    }
}
