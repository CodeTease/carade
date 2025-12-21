package core.commands.string;

import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.Carade;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class BitCountCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'bitcount' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);

        if (entry == null) {
            client.sendInteger(0);
            return;
        }

        if (entry.type != DataType.STRING) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        byte[] bytes = (byte[]) entry.getValue();
        long start = 0;
        long end = bytes.length - 1;

        if (args.size() >= 4) {
            try {
                start = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
                end = Long.parseLong(new String(args.get(3), StandardCharsets.UTF_8));
                if (start < 0) start += bytes.length;
                if (end < 0) end += bytes.length;
                if (start < 0) start = 0;
                if (end >= bytes.length) end = bytes.length - 1;
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
                return;
            }
        }

        long count = 0;
        if (start <= end) {
            for (long i = start; i <= end; i++) {
                count += Integer.bitCount(bytes[(int) i] & 0xFF);
            }
        }

        client.sendInteger(count);
    }
}
