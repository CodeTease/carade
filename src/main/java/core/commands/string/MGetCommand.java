package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MGetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("wrong number of arguments for 'mget' command");
            return;
        }
        if (client.isResp()) {
            List<byte[]> results = new ArrayList<>();
            for (int i = 1; i < args.size(); i++) {
                String key = new String(args.get(i), StandardCharsets.UTF_8);
                ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
                if (entry == null || entry.type != DataType.STRING) {
                    results.add(null);
                } else {
                    results.add((byte[]) entry.getValue());
                }
            }
            client.send(true, Resp.array(results), null);
        } else {
            List<String> results = new ArrayList<>();
            for (int i = 1; i < args.size(); i++) {
                String key = new String(args.get(i), StandardCharsets.UTF_8);
                ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
                if (entry == null || entry.type != DataType.STRING) {
                    results.add(null);
                } else {
                    results.add(new String((byte[]) entry.getValue(), StandardCharsets.UTF_8));
                }
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < results.size(); i++) {
                String val = results.get(i);
                sb.append((i+1) + ") " + (val == null ? "(nil)" : "\"" + val + "\"") + "\n");
            }
            client.send(false, null, sb.toString().trim());
        }
    }
}
