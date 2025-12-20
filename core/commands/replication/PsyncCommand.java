package core.commands.replication;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.replication.ReplicationBacklog;
import core.replication.ReplicationManager;
import core.server.WriteSequencer;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PsyncCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // 1. Snapshot offset
        ReplicationBacklog backlog = WriteSequencer.getInstance().getBacklog();
        long startOffset = backlog.getGlobalOffset();
        
        String replId = "0000000000000000000000000000000000000000"; 
        
        // 2. Generate RDB to Temp File (Snapshot)
        File tempRdb = null;
        try {
            tempRdb = File.createTempFile("carade-repl", ".rdb");
            new core.persistence.rdb.RdbEncoder().save(Carade.db, tempRdb.getAbsolutePath());
            long length = tempRdb.length();
            
            // 3. Send Header
            String resyncMsg = "FULLRESYNC " + replId + " " + startOffset;
            client.sendResponse(Resp.simpleString(resyncMsg), resyncMsg);
            
            // 4. Send RDB Bulk String Header
            String bulkHeader = "$" + length + "\r\n";
            client.sendResponse(bulkHeader.getBytes(StandardCharsets.UTF_8), null);
            
            // Send file content
            try (FileInputStream fis = new FileInputStream(tempRdb)) {
                byte[] buffer = new byte[8192];
                int n;
                while ((n = fis.read(buffer)) != -1) {
                    if (n < buffer.length) {
                        byte[] chunk = Arrays.copyOf(buffer, n);
                        client.sendResponse(chunk, null);
                    } else {
                        client.sendResponse(buffer, null);
                    }
                }
            }
            client.sendResponse("\r\n".getBytes(StandardCharsets.UTF_8), null);
            
        } catch (IOException e) {
             e.printStackTrace();
             client.sendError("ERR Replication RDB generation failed");
             return;
        } finally {
             if (tempRdb != null) tempRdb.delete();
        }
        
        // 5. Catch up from Backlog (Gap filling)
        long endOffset = backlog.getGlobalOffset();
        if (endOffset > startOffset) {
            byte[] delta = backlog.readFrom(startOffset, (int)(endOffset - startOffset));
            if (delta != null && delta.length > 0) {
                client.sendResponse(delta, null);
            }
        }
        
        // 6. Register as Replica
        ReplicationManager.getInstance().addReplica(client);
    }
}
