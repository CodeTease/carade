package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BitFieldCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'bitfield' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        List<Long> results = new ArrayList<>();
        
        // Need to lock for R-M-W cycle if we want atomicity for the whole batch
        // But executeWrite works per write operation usually.
        // For BITFIELD, it's a single atomic command.
        // So we wrap the whole logic in executeWrite if there are writes.
        // We need to parse first to see if it's read-only? No, BITFIELD is generally write-capable.
        // Let's assume write capability required.
        
        try {
            client.executeWrite(() -> {
                ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
                byte[] bytes = null;
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
                    } else if (sub.equals("SET")) {
                        String type = new String(args.get(i++), StandardCharsets.UTF_8);
                        int offset = Integer.parseInt(new String(args.get(i++), StandardCharsets.UTF_8));
                        long value = Long.parseLong(new String(args.get(i++), StandardCharsets.UTF_8));
                        
                        // Expansion logic
                        int width = getWidth(type);
                        int endBit = offset + width; // simplified (offset usually bit offset?)
                        // Redis bitfield offset can be #<index> (multiply by width) or raw.
                        // Assuming raw bit offset for simplicity or minimal impl.
                        // Wait, spec says: "offset" can be number.
                        
                        int neededBytes = (endBit + 7) / 8;
                        if (bytes.length < neededBytes) {
                             byte[] newBytes = new byte[neededBytes];
                             System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                             bytes = newBytes;
                             // Update entry ref if needed, but we need to put it back later
                        }
                        
                        long oldVal = getBitfield(bytes, type, offset);
                        results.add(oldVal);
                        setBitfield(bytes, type, offset, value);
                        
                        // We must update DB entry
                        if (entry == null) {
                             Carade.db.put(client.getDbIndex(), key, new ValueEntry(bytes, DataType.STRING, -1));
                        } else {
                             entry.setValue(bytes); // entry.value is reference to array? No, setValue updates it.
                             // Actually array ref changed if expanded.
                             // So we should re-put or setValue.
                             entry.setValue(bytes);
                             entry.touch();
                        }
                    } else if (sub.equals("INCRBY")) {
                        String type = new String(args.get(i++), StandardCharsets.UTF_8);
                        int offset = Integer.parseInt(new String(args.get(i++), StandardCharsets.UTF_8));
                        long incr = Long.parseLong(new String(args.get(i++), StandardCharsets.UTF_8));
                        
                         int width = getWidth(type);
                        int endBit = offset + width;
                        int neededBytes = (endBit + 7) / 8;
                         if (bytes.length < neededBytes) {
                             byte[] newBytes = new byte[neededBytes];
                             System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                             bytes = newBytes;
                        }
                        
                        long val = getBitfield(bytes, type, offset);
                        long newVal = val + incr;
                        // Handle overflow? Default WRAP.
                        // Simplified: standard Java math wraps.
                        setBitfield(bytes, type, offset, newVal);
                        results.add(newVal);
                        
                         if (entry == null) {
                             Carade.db.put(client.getDbIndex(), key, new ValueEntry(bytes, DataType.STRING, -1));
                        } else {
                             entry.setValue(bytes);
                             entry.touch();
                        }
                    } else if (sub.equals("OVERFLOW")) {
                        i++; // Skip argument (WRAP|SAT|FAIL) - ignoring for minimal impl
                    }
                }
            }, "BITFIELD", args.toArray()); // args logging might be verbose
            
            client.sendMixedArray(new ArrayList<>(results));
            
        } catch (RuntimeException e) {
             if (e.getMessage().equals("WRONGTYPE")) client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
             else client.sendError("ERR " + e.getMessage());
        }
    }
    
    private int getWidth(String type) {
        // type is i8, u4, etc.
        return Integer.parseInt(type.substring(1));
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
             // Sign extend
             if ((res & (1L << (width - 1))) != 0) {
                 // Negative
                 // Fill upper bits with 1s
                 long mask = -1L << width;
                 res |= mask;
             }
         }
         return res;
    }
    
    private void setBitfield(byte[] bytes, String type, int offset, long value) {
         int width = Integer.parseInt(type.substring(1));
         for (int i=0; i<width; i++) {
             int bitIdx = offset + i;
             int byteIdx = bitIdx / 8;
             int bitInByte = 7 - (bitIdx % 8);
             
             long bitVal = (value >> (width - 1 - i)) & 1;
             
             if (bitVal == 1) {
                 bytes[byteIdx] |= (1 << bitInByte);
             } else {
                 bytes[byteIdx] &= ~(1 << bitInByte);
             }
         }
    }
}
