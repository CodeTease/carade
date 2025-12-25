package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SetBitCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: SETBIT key offset value");
            return;
        }
        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] oldBit = {0};
        try {
            int offset = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            int val = Integer.parseInt(new String(args.get(3), StandardCharsets.UTF_8));
            String offsetStr = new String(args.get(2), StandardCharsets.UTF_8);
            String valStr = new String(args.get(3), StandardCharsets.UTF_8);

            if (val != 0 && val != 1) {
                client.sendError("ERR bit is not an integer or out of range");
            } else if (offset < 0) {
                client.sendError("ERR bit offset is not an integer or out of range");
            } else {
                client.executeWrite(() -> {
                    Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                        byte[] bytes;
                        if (v == null) bytes = new byte[0];
                        else if (v.type != DataType.STRING) throw new RuntimeException("WRONGTYPE");
                        else bytes = (byte[]) v.getValue();
                        
                        int byteIndex = offset / 8;
                        int bitIndex = 7 - (offset % 8);
                        
                        if (byteIndex < bytes.length) {
                            oldBit[0] = (bytes[byteIndex] >> bitIndex) & 1;
                        } else {
                            oldBit[0] = 0;
                        }
                        
                        if (byteIndex >= bytes.length) {
                            byte[] newBytes = new byte[byteIndex + 1];
                            System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                            bytes = newBytes;
                        }
                        
                        if (val == 1) bytes[byteIndex] |= (1 << bitIndex);
                        else bytes[byteIndex] &= ~(1 << bitIndex);
                        
                        ValueEntry newV = new ValueEntry(bytes, DataType.STRING, -1);
                        if (v != null) newV.expireAt = v.expireAt;
                        newV.touch();
                        return newV;
                    });
                    Carade.notifyWatchers(key);
                }, "SETBIT", key, offsetStr, valStr);
                
                client.sendInteger(oldBit[0]);
            }
        } catch (NumberFormatException e) {
            client.sendError("ERR bit offset is not an integer or out of range");
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                client.sendError(msg);
            else throw e;
        }
    }
}
