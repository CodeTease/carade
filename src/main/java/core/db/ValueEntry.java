package core.db;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import core.structs.CaradeZSet;

public class ValueEntry implements Serializable {
    public Object value; 
    public DataType type;
    public long expireAt = -1;
    public long lastAccessed;
    public int frequency = 0;
    
    public ValueEntry(Object value, DataType type, long expireAt) {
        this.value = value;
        this.type = type;
        this.expireAt = expireAt;
        this.lastAccessed = System.currentTimeMillis();
    }
    
    public void touch() {
        this.lastAccessed = System.currentTimeMillis();
        if (this.frequency < Integer.MAX_VALUE) {
            this.frequency++;
        }
    }
    
    public boolean isExpired() {
        return expireAt != -1 && System.currentTimeMillis() > expireAt;
    }
    
    public boolean isExpired(long now) {
        return expireAt != -1 && now > expireAt;
    }
    
    public long getExpireAt() { return expireAt; }
    public void setExpireAt(long expireAt) { this.expireAt = expireAt; }

    public synchronized Object getValue() {
        if (value instanceof byte[] && type != DataType.STRING) {
            inflate();
        }
        return value;
    }
    
    public synchronized void setValue(Object val) {
        this.value = val;
    }
    
    public synchronized void compress() {
        if (value instanceof byte[]) return; 
        
        try {
            if (type == DataType.LIST) {
                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) value;
                if (list.size() < 512) { 
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeInt(list.size());
                    for(String s : list) {
                        byte[] b = s.getBytes("UTF-8");
                        dos.writeInt(b.length);
                        dos.write(b);
                    }
                    this.value = bos.toByteArray();
                }
            } else if (type == DataType.HASH) {
                ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) value;
                if (map.size() < 512) {
                     ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeInt(map.size());
                    for(Map.Entry<String, String> e : map.entrySet()) {
                        byte[] k = e.getKey().getBytes("UTF-8");
                        byte[] v = e.getValue().getBytes("UTF-8");
                        dos.writeInt(k.length);
                        dos.write(k);
                        dos.writeInt(v.length);
                        dos.write(v);
                    }
                    this.value = bos.toByteArray();
                }
            }
        } catch (Exception e) {}
    }
    
    private void inflate() {
        try {
            byte[] data = (byte[]) value;
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            
            if (type == DataType.LIST) {
                int size = dis.readInt();
                ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                for(int i=0; i<size; i++) {
                    int len = dis.readInt();
                    byte[] b = new byte[len];
                    dis.readFully(b);
                    list.add(new String(b, "UTF-8"));
                }
                this.value = list;
            } else if (type == DataType.HASH) {
                int size = dis.readInt();
                ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                for(int i=0; i<size; i++) {
                     int klen = dis.readInt();
                     byte[] kb = new byte[klen];
                     dis.readFully(kb);
                     int vlen = dis.readInt();
                     byte[] vb = new byte[vlen];
                     dis.readFully(vb);
                     map.put(new String(kb, "UTF-8"), new String(vb, "UTF-8"));
                }
                this.value = map;
            }
        } catch (Exception e) {}
    }

    @SuppressWarnings("unchecked")
    public ValueEntry copy() {
        Object newVal = null;
        Object val = getValue(); // Ensure inflated
        
        switch (type) {
            case STRING:
                if (val instanceof byte[]) {
                    newVal = ((byte[]) val).clone();
                } else {
                    newVal = val; // String or Integer are immutable
                }
                break;
            case LIST:
                newVal = new ConcurrentLinkedDeque<>((ConcurrentLinkedDeque<String>) val);
                break;
            case SET:
                newVal = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
                ((Set<String>)newVal).addAll((Set<String>) val);
                break;
            case HASH:
                newVal = new ConcurrentHashMap<>((ConcurrentHashMap<String, String>) val);
                break;
            case ZSET:
                newVal = ((CaradeZSet) val).copy();
                break;
            case BLOOM:
                newVal = ((core.structs.BloomFilter) val).copy();
                break;
            case HYPERLOGLOG:
                newVal = ((core.structs.HyperLogLog) val).copy();
                break;
            case TDIGEST:
                newVal = ((core.structs.tdigest.TDigest) val).copy();
                break;
            case JSON:
                newVal = val; // Immutable structure or handled by library (Jackson JsonNode is immutable)
                break;
        }
        
        if (newVal == null) newVal = val; // Fallback
        
        return new ValueEntry(newVal, type, expireAt);
    }
}
