package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GetBitCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: GETBIT key offset");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        try {
            int offset = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
            if (entry == null) {
                client.sendInteger(0);
            } else if (entry.type != DataType.STRING) {
                client.sendError("WRONGTYPE");
            } else {
                byte[] bytes = (byte[]) entry.getValue();
                int byteIndex = offset / 8;
                int bitIndex = 7 - (offset % 8);
                
                int bit = 0;
                if (byteIndex < bytes.length) {
                    bit = (bytes[byteIndex] >> bitIndex) & 1;
                }
                client.sendInteger(bit);
            }
        } catch (NumberFormatException e) {
            client.sendError("ERR bit offset is not an integer or out of range");
        }
    }
}
