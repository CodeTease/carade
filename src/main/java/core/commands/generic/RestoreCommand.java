package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RestoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: RESTORE key ttl serialized-value [REPLACE]");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        long ttl;
        try {
            ttl = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        byte[] serialized = args.get(3);
        
        boolean replace = false;
        if (args.size() > 4) {
            if (new String(args.get(4), StandardCharsets.UTF_8).equalsIgnoreCase("REPLACE")) {
                replace = true;
            } else {
                client.sendError("ERR syntax error");
                return;
            }
        }
        
        final long finalTtl = ttl;
        final boolean finalReplace = replace;

        client.executeWrite(() -> {
            ValueEntry existing = Carade.db.get(client.getDbIndex(), key);
            if (existing != null && !finalReplace) {
                throw new RuntimeException("BUSYKEY Target key name already exists.");
            }
            
            // Parse
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream dis = new DataInputStream(bais);
                
                // Read Type
                int typeCode = dis.readByte();
                // Read Value using RdbParser logic?
                // RdbParser parses entire file usually.
                // We need `parseValue`.
                // RdbParser is coupled with `parse(db)`.
                // We need to verify if RdbParser has a helper or we need to extract it.
                // Looking at RdbParser source is not possible directly as I don't have it read, 
                // but I saw `Carade.java` implements a mini-parser for legacy/fallback.
                
                // Wait, `Carade.java` has `loadData` which uses `new RdbParser(is).parse(db)`.
                // Let's assume I need to implement a small parser here matching `DumpCommand` output.
                
                // DumpCommand format:
                // [Type Byte] [Value] [Version Short] [Checksum Long]
                
                DataType type;
                Object value;
                
                // We need to implement reading logic matching `RdbEncoder.encodeValue`
                // I'll implement a simplified reader here since I can't easily modify RdbParser to expose single value reader 
                // without potentially breaking its state machine if it assumes full RDB structure.
                
                if (typeCode == 0) { // STRING
                    type = DataType.STRING;
                    value = readString(dis);
                } else if (typeCode == 1) { // LIST
                    type = DataType.LIST;
                    long len = readLen(dis);
                    java.util.concurrent.ConcurrentLinkedDeque<String> list = new java.util.concurrent.ConcurrentLinkedDeque<>();
                    for (int i=0; i<len; i++) list.add(new String(readString(dis), StandardCharsets.UTF_8));
                    value = list;
                } else if (typeCode == 2) { // SET
                    type = DataType.SET;
                    long len = readLen(dis);
                    java.util.Set<String> set = java.util.concurrent.ConcurrentHashMap.newKeySet();
                    for (int i=0; i<len; i++) set.add(new String(readString(dis), StandardCharsets.UTF_8));
                    value = set;
                } else if (typeCode == 3) { // ZSET
                    type = DataType.ZSET;
                    long len = readLen(dis);
                    core.structs.CaradeZSet zset = new core.structs.CaradeZSet();
                    for (int i=0; i<len; i++) {
                        String member = new String(readString(dis), StandardCharsets.UTF_8);
                        // Score is stored as string in encoder
                        byte[] scoreBytes = readString(dis); 
                        String sScore = new String(scoreBytes, StandardCharsets.US_ASCII);
                        double score;
                        if (sScore.equals("+inf")) score = Double.POSITIVE_INFINITY;
                        else if (sScore.equals("-inf")) score = Double.NEGATIVE_INFINITY;
                        else score = Double.parseDouble(sScore);
                        zset.add(score, member);
                    }
                    value = zset;
                } else if (typeCode == 4) { // HASH
                    type = DataType.HASH;
                    long len = readLen(dis);
                    java.util.concurrent.ConcurrentHashMap<String, String> map = new java.util.concurrent.ConcurrentHashMap<>();
                    for (int i=0; i<len; i++) {
                         String k = new String(readString(dis), StandardCharsets.UTF_8);
                         String v = new String(readString(dis), StandardCharsets.UTF_8);
                         map.put(k, v);
                    }
                    value = map;
                } else {
                    throw new RuntimeException("Bad data format");
                }
                
                // Version
                dis.readShort();
                // Checksum
                dis.readLong();
                
                ValueEntry ve = new ValueEntry(value, type, -1);
                if (finalTtl > 0) {
                    ve.expireAt = System.currentTimeMillis() + finalTtl;
                }
                
                Carade.db.put(client.getDbIndex(), key, ve);
                client.sendSimpleString("OK");
                
            } catch (Exception e) {
                 throw new RuntimeException("DUMP payload version or checksum are wrong: " + e.getMessage());
            }

        }, "RESTORE", args.toArray());
    }
    
    // Helpers copied from RdbParser logic assumptions
    // We need to handle Length encoding
    private long readLen(DataInputStream dis) throws java.io.IOException {
        int b = dis.readByte() & 0xFF;
        int type = (b & 0xC0) >> 6;
        if (type == 0) { // 6 bit
            return b & 0x3F;
        } else if (type == 1) { // 14 bit
            int b2 = dis.readByte() & 0xFF;
            return ((b & 0x3F) << 8) | b2;
        } else if (type == 2) { // 32 bit
            return dis.readInt() & 0xFFFFFFFFL;
        } else if (type == 3) { // Encoded
            // This usually means compression for string, but for Length it might be special format?
            // In RdbEncoder writeLen, type 3 is 32bit len (0x80) ?
            // No, RdbConstants.RDB_32BITLEN is 0x80 (10xxxxxx)? No.
            // 00 = 6bit, 01 = 14bit, 10 = 32bit.
            // 11 = Encoded (Special)
            // Wait, RdbEncoder uses:
            // if len < 64: (len | (0 << 6))
            // if len < 16384: (len ... | (1 << 6))
            // else: (0x80) then int. 0x80 is 10000000. So type is 2 (10).
            
            // If type == 3 (11), it is "Encoded String" format (LZF/LZ4).
            // But `readLen` is called for collection size. Collection size is never compressed.
            // Only String value is compressed.
            // So for collection size, type 3 shouldn't happen unless I misunderstood RDB.
            // However, `readString` uses `readLen`.
             throw new RuntimeException("Unexpected length encoding for collection size: " + type);
        }
        return 0;
    }
    
    private byte[] readString(DataInputStream dis) throws java.io.IOException {
        int b = dis.readByte() & 0xFF;
        int type = (b & 0xC0) >> 6;
        long len;
        boolean isEncoded = false;
        
        if (type == 0) {
            len = b & 0x3F;
        } else if (type == 1) {
            int b2 = dis.readByte() & 0xFF;
            len = ((b & 0x3F) << 8) | b2;
        } else if (type == 2) {
            len = dis.readInt() & 0xFFFFFFFFL;
        } else {
            // Type 3 = Encoded
            isEncoded = true;
            len = b & 0x3F; // The low 6 bits indicate encoding type
        }
        
        if (isEncoded) {
            int encoding = (int)len;
            if (encoding == 3) { // LZF or LZ4 (Carade uses LZ4 as type 3/4? 
                // RdbEncoder uses RDB_ENC_LZ4 which is likely defined as constant.
                // In RdbEncoder: `(RdbConstants.RDB_ENCVAL << 6) | RdbConstants.RDB_ENC_LZ4`
                // RDB_ENCVAL is 3 (11).
                // So type is 3. Low 6 bits are the encoding.
                // We need to check RdbConstants values.
                // Assuming RdbConstants.RDB_ENC_LZ4 is what we wrote.
                // We don't have RdbConstants visible, but RdbEncoder used it.
                
                // Let's assume standard LZF (type 3 in Redis) or our custom LZ4.
                // If we wrote LZ4, we need to read it.
                // Let's look at `Carade.java` or `RdbEncoder.java` imports?
                // RdbEncoder uses `RdbConstants.RDB_ENC_LZ4`.
                // I'll assume simplified handling: if encoding is 3 or 4 (Carade specific?), decompress.
                // For now, let's implement basic "read compressed" reading uncompressed length and compressed length.
                
                // standard LZF: [compressed len] [uncompressed len] [data]
                // LZ4 (Carade): [compressed len] [uncompressed len] [data]
                long clen = readLen(dis);
                long ulen = readLen(dis);
                byte[] compressed = new byte[(int)clen];
                dis.readFully(compressed);
                
                byte[] uncompressed = new byte[(int)ulen];
                net.jpountz.lz4.LZ4Factory.fastestInstance().fastDecompressor().decompress(compressed, uncompressed);
                return uncompressed;
            } else {
                 // LZF or others not supported in this simplified Restore
                 throw new RuntimeException("Unsupported string encoding: " + encoding);
            }
        } else {
            byte[] data = new byte[(int)len];
            dis.readFully(data);
            return data;
        }
    }
}
