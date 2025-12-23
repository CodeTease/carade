package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ExpireTimeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 2) {
            client.sendError("usage: EXPIRETIME key");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry val = Carade.db.get(client.getDbIndex(), key);
        
        if (val == null) {
            client.sendInteger(-2);
            return;
        }
        
        long expireAt = val.getExpireAt();
        if (expireAt == -1) {
            client.sendInteger(-1);
        } else {
            client.sendInteger(expireAt / 1000);
        }
    }
}
