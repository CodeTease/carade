package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BitFieldRoCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'bitfield_ro' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        List<Long> results = new ArrayList<>();
        
        try {
            ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
            byte[] bytes;
            if (entry != null) {
                if (entry.type != DataType.STRING) throw new RuntimeException("WRONGTYPE");
                bytes = (byte[]) entry.getValue();
            } else {
                bytes = new byte[0];
            }

            int i = 2;
            while (i < args.size()) {
                String sub = new String(args.get(i++), StandardCharsets.UTF_8).toUpperCase();
                if (sub.equals("GET")) {
                    String type = new String(args.get(i++), StandardCharsets.UTF_8);
                    int offset = Integer.parseInt(new String(args.get(i++), StandardCharsets.UTF_8));
                    results.add(getBitfield(bytes, type, offset));
                } else {
                     client.sendError("ERR syntax error, BITFIELD_RO only supports GET");
                     return;
                }
            }
            client.sendMixedArray(new ArrayList<>(results));
        } catch (RuntimeException e) {
             if (e.getMessage().equals("WRONGTYPE")) client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
             else client.sendError("ERR " + e.getMessage());
        }
    }
    
    private long getBitfield(byte[] bytes, String type, int offset) {
         boolean signed = type.toLowerCase().startsWith("i");
         int width = Integer.parseInt(type.substring(1));
         long res = 0;
         for (int i=0; i<width; i++) {
             int bitIdx = offset + i;
             int byteIdx = bitIdx / 8;
             int bitInByte = 7 - (bitIdx % 8);
             if (byteIdx < bytes.length && ((bytes[byteIdx] >> bitInByte) & 1) == 1) {
                 res |= (1L << (width - 1 - i));
             }
         }
         
         if (signed) {
             if ((res & (1L << (width - 1))) != 0) {
                 long mask = -1L << width;
                 res |= mask;
             }
         }
         return res;
    }
}
