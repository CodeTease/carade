package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GetSetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("wrong number of arguments for 'getset' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        byte[] newValue = args.get(2);
        final byte[][] oldValRef = {null};

        client.executeWrite(() -> {
            Carade.db.getStore(client.dbIndex).compute(key, (k, v) -> {
                if (v == null) {
                    oldValRef[0] = null;
                    return new ValueEntry(newValue, DataType.STRING, -1);
                } else if (v.type != DataType.STRING) {
                    throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    oldValRef[0] = (byte[]) v.getValue();
                    ValueEntry newV = new ValueEntry(newValue, DataType.STRING, -1);
                    // GETSET resets TTL? Redis documentation says: "The TTL of the key is discarded."
                    // Wait, GETSET sets the value. SET command without params removes TTL. 
                    // Let's assume yes, it behaves like SET in this regard.
                    return newV;
                }
            });
            Carade.notifyWatchers(key);
        }, "GETSET", key, newValue);

        // Handle response based on what happened
        // Note: exceptions in compute are propagated wrapped in RuntimeException usually, but here we throw RuntimeException directly.
        // But executeWrite swallows exceptions? No, ClientHandler.executeWrite calls WriteSequencer which just runs it.
        // Wait, ClientHandler.executeWrite executes the runnable. If it throws, it bubbles up.
        // However, we need to catch it to send error to client if it was WRONGTYPE.
        // But here we are inside the executeWrite lambda. The exception will be thrown out of executeWrite.
        // We should wrap the executeWrite call in try-catch in the outer block?
        // Ah, looking at ClientHandler.executeWrite implementation: It just calls WriteSequencer.executeWrite.
        // WriteSequencer.executeWrite executes the task.
        // If an exception occurs, it will bubble up to here.
        
        // Wait, ClientHandler code catches exceptions in the run loop.
        // But `oldValRef` needs to be sent.
        
        // Let's refine the try-catch block logic.
        // Since `executeWrite` is void, we need to handle the logic flow carefully.
        
        // Actually, if we look at ClientHandler structure, exceptions are caught in the main loop.
        // But we want to send the response *after* the write.
        
        // If WRONGTYPE was thrown, we won't reach here?
        // No, we need to catch it.
        
        // Re-reading ClientHandler.java:
        /*
            } catch (Exception e) { send(out, isResp, Resp.error("ERR " + e.getMessage()), "(error) ERR " + e.getMessage()); }
        */
        // So if we throw RuntimeException("WRONGTYPE ..."), the main loop catches it and sends ERR WRONGTYPE ...
        // So we just need to ensure we don't send response if we threw.
        
        // Wait, if I throw exception inside executeWrite, it bubbles up. 
        // So I can't send the "old value" response if I throw.
        // But I only want to send old value if NO exception.
        
        if (oldValRef[0] == null) {
            // It might be because key didn't exist OR because of WRONGTYPE (if we didn't catch it).
            // We can't distinguish easily unless we check `v.type` *before* throwing or using a flag.
            // But if we throw, execution stops here and goes to catch block in ClientHandler.run().
            // So we are good. If we reach this line, no exception was thrown.
            
            // However, we need to know if the key existed.
            // compute returns the new value.
            // But we captured old value in oldValRef[0].
            
            // If oldValRef[0] is null, it means key didn't exist.
            client.sendNull();
        } else {
            client.sendBulkString(new String(oldValRef[0], StandardCharsets.UTF_8));
        }
    }
}
