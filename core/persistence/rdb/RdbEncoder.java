package core.persistence.rdb;

import core.db.CaradeDatabase;
import core.db.DataType;
import core.db.ValueEntry;
import core.structs.CaradeZSet;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class RdbEncoder {

    public void save(CaradeDatabase db, String filepath) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filepath)))) {
            encode(db, dos);
        }
    }

    public void encode(CaradeDatabase db, DataOutputStream dos) throws IOException {
        // Header
        dos.write(RdbConstants.RDB_MAGIC.getBytes(StandardCharsets.US_ASCII));
        dos.write("0009".getBytes(StandardCharsets.US_ASCII));
        
        // Aux (optional but good practice)
        dos.write(RdbConstants.RDB_OPCODE_AUX);
        writeString(dos, "redis-ver");
        writeString(dos, "6.0.0"); // Pretend to be Redis 6
        
        for (int i = 0; i < CaradeDatabase.DB_COUNT; i++) {
            if (db.size(i) == 0) continue;

            // Select DB
            dos.write(RdbConstants.RDB_OPCODE_SELECTDB);
            writeLen(dos, i);
            
            // Resize DB (optional)
            dos.write(RdbConstants.RDB_OPCODE_RESIZEDB);
            writeLen(dos, db.size(i));
            writeLen(dos, 0); // Expires size (we don't track separately easily, so 0)
            
            for (Map.Entry<String, ValueEntry> entry : db.entrySet(i)) {
                ValueEntry v = entry.getValue();
                if (v.isExpired()) continue;
                
                // Expire
                if (v.expireAt != -1) {
                    dos.write(RdbConstants.RDB_OPCODE_EXPIRETIMEMS);
                    dos.writeLong(v.expireAt);
                }
                
                // Type
                int type = 0;
                if (v.type == DataType.STRING) type = RdbConstants.RDB_TYPE_STRING;
                else if (v.type == DataType.LIST) type = RdbConstants.RDB_TYPE_LIST;
                else if (v.type == DataType.SET) type = RdbConstants.RDB_TYPE_SET;
                else if (v.type == DataType.ZSET) type = RdbConstants.RDB_TYPE_ZSET; // Use 3 (string score) for better compatibility
                else if (v.type == DataType.HASH) type = RdbConstants.RDB_TYPE_HASH;
                
                dos.write(type);
                
                // Key
                writeString(dos, entry.getKey());
                
                // Value
                if (v.type == DataType.STRING) {
                    writeString(dos, (byte[]) v.getValue());
                } else if (v.type == DataType.LIST) {
                    ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
                    writeLen(dos, list.size());
                    for (String s : list) writeString(dos, s);
                } else if (v.type == DataType.SET) {
                    Set<String> set = (Set<String>) v.getValue();
                    writeLen(dos, set.size());
                    for (String s : set) writeString(dos, s);
                } else if (v.type == DataType.ZSET) {
                    CaradeZSet zset = (CaradeZSet) v.getValue();
                    writeLen(dos, zset.size());
                    for (Map.Entry<String, Double> e : zset.scores.entrySet()) {
                        writeString(dos, e.getKey());
                        
                        // Write score as string to avoid endianness issues
                        double score = e.getValue();
                        String sScore;
                        if (Double.isInfinite(score)) {
                            sScore = (score > 0) ? "+inf" : "-inf"; 
                        } else {
                            sScore = String.valueOf(score);
                        }
                        
                        byte[] bScore = sScore.getBytes(StandardCharsets.US_ASCII);
                        dos.write(bScore.length); // 8 bit len
                        dos.write(bScore);
                    }
                } else if (v.type == DataType.HASH) {
                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
                    writeLen(dos, map.size());
                    for (Map.Entry<String, String> e : map.entrySet()) {
                        writeString(dos, e.getKey());
                        writeString(dos, e.getValue());
                    }
                }
            }
        }
        
        // EOF
        dos.write(RdbConstants.RDB_OPCODE_EOF);
        
        // Checksum (8 bytes) - 0 means disabled
        dos.writeLong(0);
    }
    
    private void writeLen(DataOutputStream dos, long len) throws IOException {
        if (len < 64) {
            dos.write((int) (len | (RdbConstants.RDB_6BITLEN << 6)));
        } else if (len < 16384) {
            dos.write((int) (((len >> 8) & 0xFF) | (RdbConstants.RDB_14BITLEN << 6)));
            dos.write((int) (len & 0xFF));
        } else {
            dos.write(RdbConstants.RDB_32BITLEN);
            dos.writeInt((int) len); // Technically big endian here is fine as per our Parser logic
        }
    }
    
    private void writeString(DataOutputStream dos, String s) throws IOException {
        writeString(dos, s.getBytes(StandardCharsets.UTF_8));
    }
    
    private void writeString(DataOutputStream dos, byte[] bytes) throws IOException {
        writeLen(dos, bytes.length);
        dos.write(bytes);
    }
}
