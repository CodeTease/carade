package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.persistence.rdb.RdbEncoder;
import core.protocol.Resp;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MigrateCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 6) {
            client.sendError("ERR wrong number of arguments for 'migrate' command");
            return;
        }
        
        try {
            String host = new String(args.get(1), StandardCharsets.UTF_8);
            int port = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            String key = new String(args.get(3), StandardCharsets.UTF_8);
            int timeout = Integer.parseInt(new String(args.get(5), StandardCharsets.UTF_8));
            
            boolean copy = false;
            boolean replace = false;
            
            for (int i = 6; i < args.size(); i++) {
                String opt = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
                if (opt.equals("COPY")) copy = true;
                else if (opt.equals("REPLACE")) replace = true;
            }
            
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v == null) {
                client.sendSimpleString("NOKEY");
                return;
            }
            
            // Dump logic
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            // Minimal RDB dump
            // Type
            int type = 0; // Simplified type mapping
            switch (v.type) {
                case STRING: type = 0; break;
                case LIST: type = 1; break;
                case SET: type = 2; break;
                case ZSET: type = 3; break; // ZSET as type 3 (Set) in older redis? No, ZSET is 3/4. RdbConstants.RDB_TYPE_ZSET is 3 (generic) or 4 (zipped).
                // We should match RestoreCommand expectations. RestoreCommand calls RdbParser.
                // RdbParser expects standard types.
                // Let's use 3 for ZSET.
                case HASH: type = 4; break;
                default: type = 0;
            }
            dos.writeByte(type);
            new RdbEncoder().encodeValue(dos, v);
            dos.writeShort(9); // Version
            dos.writeLong(0); // Checksum
            
            byte[] dumpPayload = baos.toByteArray();
            
            // Connect and Send
            try (Socket socket = new Socket(host, port)) {
                socket.setSoTimeout(timeout);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                
                // RESTORE key ttl value [REPLACE]
                List<byte[]> cmdParts = new ArrayList<>();
                cmdParts.add("RESTORE".getBytes(StandardCharsets.UTF_8));
                cmdParts.add(key.getBytes(StandardCharsets.UTF_8));
                cmdParts.add("0".getBytes(StandardCharsets.UTF_8)); // TTL 0 = no expire
                cmdParts.add(dumpPayload);
                if (replace) cmdParts.add("REPLACE".getBytes(StandardCharsets.UTF_8));
                
                out.write(Resp.array(cmdParts));
                out.flush();
                
                int b = in.read();
                if (b == '-') { // Error
                    client.sendError("ERR Target instance returned error");
                    return;
                }
                
                if (!copy) {
                    client.executeWrite(() -> {
                        Carade.db.remove(client.getDbIndex(), key);
                    }, "DEL", key);
                }
                
                client.sendSimpleString("OK");
                
            } catch (Exception e) {
                client.sendError("ERR Migration failed: " + e.getMessage());
            }
            
        } catch (Exception e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
}
