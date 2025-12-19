package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PexpireCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: PEXPIRE key milliseconds");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        try {
            long ms = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
            final int[] ret = {0};
            Carade.db.getStorage().computeIfPresent(key, (k, v) -> {
                v.setExpireAt(System.currentTimeMillis() + ms);
                ret[0] = 1;
                return v;
            });
            if (ret[0] == 1) Carade.aofHandler.log("PEXPIRE", key, String.valueOf(ms));
            client.sendResponse(Resp.integer(ret[0]), "(integer) " + ret[0]);
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
