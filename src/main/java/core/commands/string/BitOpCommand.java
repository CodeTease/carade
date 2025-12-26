package core.commands.string;

import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.Carade;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class BitOpCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("ERR wrong number of arguments for 'bitop' command");
            return;
        }

        String op = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();
        String destKey = new String(args.get(2), StandardCharsets.UTF_8);
        
        // Check OP validity
        if (!op.equals("AND") && !op.equals("OR") && !op.equals("XOR") && !op.equals("NOT")) {
             client.sendError("ERR syntax error");
             return;
        }
        
        if (op.equals("NOT") && args.size() != 4) {
             client.sendError("ERR BITOP NOT must be called with a single source key.");
             return;
        }

        // Collect source keys
        byte[][] srcValues = new byte[args.size() - 3][];
        int maxLen = 0;
        
        for (int i = 3; i < args.size(); i++) {
            String key = new String(args.get(i), StandardCharsets.UTF_8);
            ValueEntry entry = Carade.db.get(client.dbIndex, key);
            if (entry == null) {
                srcValues[i-3] = null;
            } else if (entry.type != DataType.STRING) {
                client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            } else {
                srcValues[i-3] = (byte[]) entry.getValue();
                if (srcValues[i-3].length > maxLen) maxLen = srcValues[i-3].length;
            }
        }
        
        byte[] res = new byte[maxLen];
        
        if (op.equals("NOT")) {
            byte[] src = srcValues[0];
            if (src != null) {
                res = new byte[src.length];
                for (int i=0; i<src.length; i++) {
                    res[i] = (byte) ~src[i];
                }
            } else {
                res = new byte[0]; // NOT of empty/missing is empty? Redis says missing key is 0 bytes. NOT 0 is 0? No, NOT is unary. Redis documentation says "missing keys are considered a stream of zero bytes". NOT of 00000000 is 11111111. But how long? Redis uses the length of the input key. So if key missing, length 0, result empty.
            }
        } else {
            // AND, OR, XOR
            for (int i = 0; i < maxLen; i++) {
                int b = 0;
                // Initialize with first operand value (or 0)
                // BUT for AND: 1 & 0 = 0.
                // It's easier to follow the loop.
                
                // First value
                int v0 = (srcValues[0] != null && i < srcValues[0].length) ? (srcValues[0][i] & 0xFF) : 0;
                b = v0;
                
                for (int j = 1; j < srcValues.length; j++) {
                    int v = (srcValues[j] != null && i < srcValues[j].length) ? (srcValues[j][i] & 0xFF) : 0;
                    if (op.equals("AND")) b &= v;
                    else if (op.equals("OR")) b |= v;
                    else if (op.equals("XOR")) b ^= v;
                }
                res[i] = (byte) b;
            }
        }
        
        // Store result
        // We need to use executeWrite to ensure replication/AOF if we had that fully wired up for commands
        // But Command.execute is generally running inside lock if identified as write?
        // Wait, ClientHandler identifies write commands via string list.
        // We need to add BITOP to isWriteCommand list in ClientHandler? 
        // The plan says "Register in CommandRegistry".
        // ClientHandler checks `isWriteCommand` for `SET`, `DEL` etc.
        // If I implement it as Command, ClientHandler calls `cmdObj.execute`.
        // BUT ClientHandler determines locking strategy BEFORE calling execute.
        // So I must ensure BITOP is seen as write command, OR I handle locking myself.
        // ClientHandler code:
        // boolean needsWriteLock = isWriteCommand(cmd) || ...
        // if (needsWriteLock) ... executeCommand ...
        // So I need to update `isWriteCommand` in `ClientHandler.java` as well!
        
        final byte[] finalRes = res;
        client.executeWrite(() -> {
            if (finalRes.length == 0) {
                Carade.db.remove(client.dbIndex, destKey);
            } else {
                Carade.db.put(client.dbIndex, destKey, new ValueEntry(finalRes, DataType.STRING, -1));
            }
            Carade.notifyWatchers(destKey);
        }, "BITOP", (Object[]) args.stream().skip(1).map(b -> new String(b, StandardCharsets.UTF_8)).toArray());
        
        client.sendInteger(res.length);
    }
}
