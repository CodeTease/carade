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
import java.util.Collections;
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
            // We use RdbEncoder to serialize just this key-value.
            // But RdbEncoder serializes entire DB structure usually.
            // We need a fragment serializer. 
            // RdbEncoder.encode writes header/etc.
            // We need a way to invoke internal write logic for a specific type.
            // Since RdbEncoder has private methods, we might need to expose them or duplicate logic.
            // Let's modify RdbEncoder to expose `encodeValue`?
            // Or create a mini encoder here.
            
            // Re-implementing simplified DUMP format (Redis RDB version 9 compatible)
            // Or just use Java Serialization? No, DUMP is supposed to be opaque but RDB format usually.
            
            // Let's rely on a helper in RdbEncoder if possible, but I cannot change it easily now without context.
            // I will implement a minimal RDB serialization for the value type here.
            // This duplicates logic from RdbEncoder but safe for a single command.
            
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
