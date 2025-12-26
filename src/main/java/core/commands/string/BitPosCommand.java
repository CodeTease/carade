package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class BitPosCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: BITPOS key bit [start] [end]");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        int bit;
        try {
            bit = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        if (bit != 0 && bit != 1) {
            client.sendError("ERR The bit argument must be 1 or 0.");
            return;
        }

        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        byte[] data;
        if (v == null) {
            data = new byte[0];
        } else if (v.type != DataType.STRING) {
             client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
             return;
        } else {
             data = (byte[]) v.getValue();
        }
        
        int start = 0;
        int end = data.length - 1;
        
        if (args.size() > 3) {
            try {
                start = Integer.parseInt(new String(args.get(3), StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
                return;
            }
        }
        if (args.size() > 4) {
            try {
                end = Integer.parseInt(new String(args.get(4), StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
                return;
            }
        }
        
        // Adjust indices
        if (start < 0) start = data.length + start;
        if (end < 0) end = data.length + end;
        if (start < 0) start = 0;
        if (end >= data.length) end = data.length - 1;
        
        if (start > end) {
            client.sendInteger(-1);
            return;
        }
        
        long pos = -1;
        // Byte loop
        for (int i = start; i <= end; i++) {
            int b = data[i] & 0xFF;
            if (bit == 1) {
                if (b == 0) continue;
                // Find first 1
                for (int j = 0; j < 8; j++) {
                    // 128 (10000000) is index 0. 
                    if ((b & (1 << (7 - j))) != 0) {
                        pos = ((long) i * 8) + j;
                        break;
                    }
                }
            } else {
                if (b == 255) continue;
                // Find first 0
                for (int j = 0; j < 8; j++) {
                    if ((b & (1 << (7 - j))) == 0) {
                        pos = ((long) i * 8) + j;
                        break;
                    }
                }
            }
            if (pos != -1) break;
        }
        
        // Special case: BITPOS key 0. If entire range is 1s, and we search 0.
        // Redis behavior: if bit is 0, and we don't find it in existing bytes:
        // If end was given, return -1.
        // If end was NOT given, return length * 8 (virtual padding with 0s).
        
        if (pos == -1 && bit == 0 && args.size() <= 4) {
             // If data was empty, return 0? No, if key not exists, return 0.
             if (data.length == 0) {
                 pos = 0;
             } else {
                 // Check if we scanned until the real end of buffer
                 // If start > length, conceptually padded with 0s.
                 // But here we bound start/end to data.length.
                 // If we finished loop and didn't find 0, it means all bits were 1.
                 // The "next" bit (virtual) is 0.
                 pos = (long) (end + 1) * 8;
             }
        }

        client.sendInteger(pos);
    }
}
