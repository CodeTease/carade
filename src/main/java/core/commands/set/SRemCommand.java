package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

public class SRemCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: SREM key member");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String member = new String(args.get(2), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        client.executeWrite(() -> {
            Carade.db.getStore(client.getDbIndex()).computeIfPresent(key, (k, v) -> {
                if (v.type == DataType.SET) {
                    Set<String> set = (Set<String>) v.getValue();
                    if (set.remove(member)) ret[0] = 1;
                    if (set.isEmpty()) return null;
                }
                return v;
            });
            if (ret[0] == 1) {
                 Carade.notifyWatchers(key);
            }
        }, "SREM", key, member);

        client.sendInteger(ret[0]);
    }
}
