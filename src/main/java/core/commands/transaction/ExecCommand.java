package core.commands.transaction;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;

public class ExecCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (!client.isInTransaction()) {
            client.sendError("ERR EXEC without MULTI");
        } else {
            client.setInTransaction(false);
            
            if (client.isTransactionDirty()) {
                client.sendNull();
                client.clearTransactionQueue();
                client.setTransactionDirty(false);
                client.unwatchAll();
                return;
            }

            client.unwatchAll();
            client.setTransactionDirty(false);
            
            List<List<byte[]>> queue = client.getTransactionQueue();
            
            if (queue.isEmpty()) {
                client.sendArray(Collections.emptyList());
            } else {
                Carade.globalRWLock.writeLock().lock();
                try {
                    // We need to capture output of executed commands
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    String header = "*" + queue.size() + "\r\n";
                    buffer.write(header.getBytes());
                    
                    client.setCaptureBuffer(buffer);
                    try {
                        for (List<byte[]> queuedCmd : queue) {
                            client.executeCommand(queuedCmd, null, client.isResp());
                        }
                    } finally {
                        client.setCaptureBuffer(null);
                    }
                    // Write raw bytes to Netty context
                    client.writeDirect(Unpooled.wrappedBuffer(buffer.toByteArray()));
                } catch (Exception e) {
                     client.sendError("ERR " + e.getMessage());
                } finally {
                    Carade.globalRWLock.writeLock().unlock();
                }
            }
        }
    }
}
