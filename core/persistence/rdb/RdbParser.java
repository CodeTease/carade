package core.persistence.rdb;

import core.db.CaradeDatabase;
import core.db.DataType;
import core.db.ValueEntry;
import core.structs.CaradeZSet;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RdbParser {
    private final DataInputStream in;

    public RdbParser(InputStream inputStream) {
        this.in = new DataInputStream(inputStream);
    }

    private byte[] readBytes(int len) throws IOException {
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    public long loadLen(MutableBoolean isEncoded) throws IOException {
        int first = in.readUnsignedByte();
        int type = (first & 0xC0) >> 6;
        int val = first & 0x3F;

        if (type == RdbConstants.RDB_ENCVAL) {
            isEncoded.value = true;
            return val;
        }

        isEncoded.value = false;
        if (type == RdbConstants.RDB_6BITLEN) {
            return val;
        } else if (type == RdbConstants.RDB_14BITLEN) {
            return ((val) << 8) | in.readUnsignedByte();
        } else if (first == RdbConstants.RDB_32BITLEN) { 
             return in.readInt() & 0xFFFFFFFFL;
        } else {
             if (first == 0x81) {
                 return in.readLong();
             }
             return in.readInt() & 0xFFFFFFFFL; 
        }
    }
    
    public long loadLen() throws IOException {
        MutableBoolean b = new MutableBoolean();
        return loadLen(b);
    }

    public byte[] loadString() throws IOException {
        MutableBoolean isEncoded = new MutableBoolean();
        long len = loadLen(isEncoded);
        
        if (isEncoded.value) {
            switch ((int)len) {
                case RdbConstants.RDB_ENC_INT8:
                    return String.valueOf(in.readByte()).getBytes(StandardCharsets.UTF_8);
                case RdbConstants.RDB_ENC_INT16:
                    return String.valueOf(in.readShort()).getBytes(StandardCharsets.UTF_8);
                case RdbConstants.RDB_ENC_INT32:
                    return String.valueOf(in.readInt()).getBytes(StandardCharsets.UTF_8);
                case RdbConstants.RDB_ENC_LZF:
                    long clen = loadLen();
                    long ulen = loadLen();
                    byte[] c = readBytes((int)clen);
                    byte[] u = new byte[(int)ulen];
                    LZF.expand(c, (int)clen, u, (int)ulen);
                    return u;
                default:
                    throw new IOException("Unknown string encoding: " + len);
            }
        }

        byte[] b = new byte[(int)len];
        in.readFully(b);
        return b;
    }
    
    private List<String> loadZipList(byte[] blob) throws IOException {
        if (blob == null) blob = loadString();
        // Simple ziplist parser
        // <zlbytes><zltail><zllen><entry>...<zlend>
        ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
        int zlbytes = bb.getInt();
        int zltail = bb.getInt();
        int zllen = bb.getShort() & 0xFFFF;
        
        List<String> entries = new ArrayList<>();
        int pos = 10; // 4+4+2
        
        while (pos < blob.length) {
            int b = blob[pos++] & 0xFF;
            if (b == 0xFF) break; // zlend
            
            // prev len
            int prevLen;
            if (b < 254) {
                prevLen = b;
            } else {
                // 254 means next 4 bytes
                prevLen = bb.getInt(pos); // Read 4 bytes at pos
                pos += 4;
            }
            
            // entry
            int entryHeader = blob[pos++] & 0xFF;
            if ((entryHeader & 0xC0) == 0x00) { // 00pppppp: string, length = pppppp
                int len = entryHeader & 0x3F;
                byte[] str = new byte[len];
                System.arraycopy(blob, pos, str, 0, len);
                entries.add(new String(str, StandardCharsets.UTF_8));
                pos += len;
            } else if ((entryHeader & 0xC0) == 0x40) { // 01pppppp|qqqqqqqq: string, 14 bit
                int len = ((entryHeader & 0x3F) << 8) | (blob[pos++] & 0xFF);
                byte[] str = new byte[len];
                System.arraycopy(blob, pos, str, 0, len);
                entries.add(new String(str, StandardCharsets.UTF_8));
                pos += len;
            } else if ((entryHeader & 0xC0) == 0x80) { // 10______: string, 32 bit len
                 int actualLen = (blob[pos] & 0xFF) << 24 | (blob[pos+1] & 0xFF) << 16 | (blob[pos+2] & 0xFF) << 8 | (blob[pos+3] & 0xFF);
                 pos += 4;
                 byte[] str = new byte[actualLen];
                 System.arraycopy(blob, pos, str, 0, actualLen);
                 entries.add(new String(str, StandardCharsets.UTF_8));
                 pos += actualLen;
            } else if ((entryHeader & 0xC0) == 0xC0) { // 11... Integer
                 int v = entryHeader & 0xF0;
                 if (v == 0xC0) {
                     short val = bb.getShort(pos); pos += 2;
                     entries.add(String.valueOf(val));
                 } else if (v == 0xD0) {
                     int val = bb.getInt(pos); pos += 4;
                     entries.add(String.valueOf(val));
                 } else if (v == 0xE0) {
                     long val = bb.getLong(pos); pos += 8;
                     entries.add(String.valueOf(val));
                 } else if (v == 0xF0) {
                     int b0 = blob[pos++] & 0xFF;
                     int b1 = blob[pos++] & 0xFF;
                     int b2 = blob[pos++] & 0xFF;
                     int val = (b2 << 16) | (b1 << 8) | b0; // Little endian 24 bit
                     if ((val & 0x800000) != 0) val |= 0xFF000000; // Sign extend
                     entries.add(String.valueOf(val));
                 } else {
                     int imm = entryHeader & 0x0F;
                     if (imm == 0xFE) { // 8 bit signed
                         byte val = blob[pos++];
                         entries.add(String.valueOf(val));
                     } else {
                         int val = imm - 1;
                         entries.add(String.valueOf(val));
                     }
                 }
            }
        }
        return entries;
    }
    
    private List<String> loadIntSet() throws IOException {
        byte[] blob = loadString();
        ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
        int encoding = bb.getInt();
        int len = bb.getInt();
        List<String> list = new ArrayList<>();
        
        for (int i=0; i<len; i++) {
            if (encoding == 2) list.add(String.valueOf(bb.getShort()));
            else if (encoding == 4) list.add(String.valueOf(bb.getInt()));
            else if (encoding == 8) list.add(String.valueOf(bb.getLong()));
        }
        return list;
    }
    
    private List<String> loadQuickList() throws IOException {
        long len = loadLen(); // Number of ziplists
        List<String> entries = new ArrayList<>();
        for (int i=0; i<len; i++) {
            byte[] blob = loadString(); // The ziplist blob
            entries.addAll(loadZipList(blob));
        }
        return entries;
    }

    public ValueEntry loadObject(int type) throws IOException {
        if (type == RdbConstants.RDB_TYPE_STRING) {
            byte[] val = loadString();
            return new ValueEntry(val, DataType.STRING, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_LIST) {
            long len = loadLen();
            ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
            for (int i = 0; i < len; i++) {
                list.add(new String(loadString(), StandardCharsets.UTF_8));
            }
            return new ValueEntry(list, DataType.LIST, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_SET) {
            long len = loadLen();
            Set<String> set = ConcurrentHashMap.newKeySet();
            for (int i = 0; i < len; i++) {
                set.add(new String(loadString(), StandardCharsets.UTF_8));
            }
            return new ValueEntry(set, DataType.SET, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_ZSET || type == RdbConstants.RDB_TYPE_ZSET_2) {
            long len = loadLen();
            CaradeZSet zset = new CaradeZSet();
            for (int i = 0; i < len; i++) {
                String member = new String(loadString(), StandardCharsets.UTF_8);
                double score;
                if (type == RdbConstants.RDB_TYPE_ZSET_2) {
                    score = in.readDouble();
                } else {
                    String scoreStr = new String(loadString(), StandardCharsets.UTF_8);
                    try {
                        score = Double.parseDouble(scoreStr);
                    } catch (NumberFormatException e) {
                        if (scoreStr.equalsIgnoreCase("inf") || scoreStr.equalsIgnoreCase("+inf")) score = Double.POSITIVE_INFINITY;
                        else if (scoreStr.equalsIgnoreCase("-inf")) score = Double.NEGATIVE_INFINITY;
                        else score = 0;
                    }
                }
                zset.add(score, member);
            }
            return new ValueEntry(zset, DataType.ZSET, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_HASH) {
            long len = loadLen();
            ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
            for (int i = 0; i < len; i++) {
                String k = new String(loadString(), StandardCharsets.UTF_8);
                String v = new String(loadString(), StandardCharsets.UTF_8);
                map.put(k, v);
            }
            return new ValueEntry(map, DataType.HASH, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_LIST_ZIPLIST) {
            List<String> entries = loadZipList(null);
            ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>(entries);
            return new ValueEntry(list, DataType.LIST, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_SET_INTSET) {
            List<String> entries = loadIntSet();
            Set<String> set = ConcurrentHashMap.newKeySet();
            set.addAll(entries);
            return new ValueEntry(set, DataType.SET, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_HASH_ZIPLIST) {
            List<String> entries = loadZipList(null);
            ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
            for (int i=0; i<entries.size(); i+=2) {
                map.put(entries.get(i), entries.get(i+1));
            }
            return new ValueEntry(map, DataType.HASH, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_ZSET_ZIPLIST) {
            List<String> entries = loadZipList(null);
            CaradeZSet zset = new CaradeZSet();
            for (int i=0; i<entries.size(); i+=2) {
                String member = entries.get(i);
                double score = Double.parseDouble(entries.get(i+1));
                zset.add(score, member);
            }
            return new ValueEntry(zset, DataType.ZSET, -1);
            
        } else if (type == RdbConstants.RDB_TYPE_LIST_QUICKLIST) {
            List<String> entries = loadQuickList();
            ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>(entries);
            return new ValueEntry(list, DataType.LIST, -1);
            
        } else {
            throw new IOException("Unsupported RDB type: " + type);
        }
    }

    public static class MutableBoolean {
        public boolean value;
    }
    
    public void parse(CaradeDatabase db) throws IOException {
        byte[] magic = new byte[5];
        in.readFully(magic);
        if (!Arrays.equals(magic, RdbConstants.RDB_MAGIC.getBytes(StandardCharsets.US_ASCII))) {
            throw new IOException("Invalid RDB Magic");
        }
        
        byte[] ver = new byte[4];
        in.readFully(ver); // Version (e.g. 0009) - we can ignore strict check for now
        
        long expireAt = -1;
        int dbIndex = 0;
        
        while (true) {
            int type = in.read();
            if (type == -1) break; // EOF check redundant with RDB_OPCODE_EOF but good for safety
            
            if (type == RdbConstants.RDB_OPCODE_EOF) {
                break;
            } else if (type == RdbConstants.RDB_OPCODE_SELECTDB) {
                dbIndex = (int) loadLen(); 
                if (dbIndex >= CaradeDatabase.DB_COUNT) {
                     // If file has more DBs than we support, we just wrap or clamp? 
                     // Redis default 16. Let's clamp or ignore. 
                     // Actually better to just use % or ensure RDB comes from compatible source.
                     // For now, let's clamp.
                     dbIndex = dbIndex % CaradeDatabase.DB_COUNT;
                }
            } else if (type == RdbConstants.RDB_OPCODE_RESIZEDB) {
                loadLen(); // db_size
                loadLen(); // expires_size
            } else if (type == RdbConstants.RDB_OPCODE_AUX) {
                // Key Value pair for AUX
                loadString(); // key
                loadString(); // value
            } else if (type == RdbConstants.RDB_OPCODE_EXPIRETIME) {
                expireAt = (in.readInt() & 0xFFFFFFFFL) * 1000;
            } else if (type == RdbConstants.RDB_OPCODE_EXPIRETIMEMS) {
                expireAt = in.readLong();
            } else {
                // Key-Value pair
                String key = new String(loadString(), StandardCharsets.UTF_8);
                ValueEntry val = loadObject(type);
                val.expireAt = expireAt;
                
                if (!val.isExpired()) {
                    db.put(dbIndex, key, val);
                }
                
                expireAt = -1; // Reset expire
            }
        }
    }
}
