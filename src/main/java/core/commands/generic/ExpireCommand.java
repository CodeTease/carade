package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ExpireCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: EXPIRE key seconds");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        try {
            long seconds = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
            long expireAt = System.currentTimeMillis() + (seconds * 1000);
            
            final int[] ret = {0};
            
            // Transform EXPIRE to PEXPIREAT (absolute time) for AOF/Replica
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).computeIfPresent(key, (k, v) -> {
                    v.expireAt = expireAt;
                    ret[0] = 1;
                    return v;
                });
            }, "PEXPIREAT", key, String.valueOf(expireAt));

            client.sendInteger(ret[0]);
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
