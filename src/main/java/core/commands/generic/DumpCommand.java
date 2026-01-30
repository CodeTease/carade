package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.persistence.rdb.RdbEncoder;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DumpCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 2) {
            client.sendError("usage: DUMP key");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        
        if (v == null) {
            client.sendNull();
            return;
        }

        try {
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // Type
            int type = 0;
            if (v.type == DataType.STRING) type = 0;
            else if (v.type == DataType.LIST) type = 1;
            else if (v.type == DataType.SET) type = 2;
            else if (v.type == DataType.ZSET) type = 3; 
            else if (v.type == DataType.HASH) type = 4;
            
            dos.writeByte(type);
            
            // Value
            new RdbEncoder().encodeValue(dos, v);
            
            // Version (Redis 6)
            dos.writeShort(9); // Version 9
            
            // Checksum (8 bytes) - we can just put 0 (disabled)
            dos.writeLong(0);
            
            byte[] data = baos.toByteArray();
            
            // Redis DUMP returns "serialized value".
            // It actually returns the payload.
            
            client.send(true, core.protocol.Resp.bulkString(data), null); // Manually sending bytes
            
        } catch (Exception e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
}
