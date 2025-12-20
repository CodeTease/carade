package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ExpireAtCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: EXPIREAT key timestamp");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        try {
            long timestamp = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
            final int[] ret = {0};
            Carade.db.getStorage(client.dbIndex).computeIfPresent(key, (k, v) -> {
                v.setExpireAt(timestamp * 1000);
                ret[0] = 1;
                return v;
            });
            if (ret[0] == 1) Carade.aofHandler.log("EXPIREAT", key, String.valueOf(timestamp));
            client.sendResponse(Resp.integer(ret[0]), "(integer) " + ret[0]);
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
