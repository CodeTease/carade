package core.persistence;

import core.db.CaradeDatabase;
import core.db.DataType;
import core.db.ValueEntry;
import core.persistence.rdb.RdbEncoder;
import core.persistence.rdb.RdbParser;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class PersistenceTest {

    @Test
    public void testRdbReload() throws IOException {
        CaradeDatabase db = CaradeDatabase.getInstance();
        db.clearAll();
        
        // Populate
        db.put(0, "s", new ValueEntry("str".getBytes(StandardCharsets.UTF_8), DataType.STRING, -1));
        
        ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
        list.add("l1"); list.add("l2");
        db.put(0, "l", new ValueEntry(list, DataType.LIST, -1));
        
        ConcurrentHashMap<String, String> hash = new ConcurrentHashMap<>();
        hash.put("f1", "v1");
        db.put(0, "h", new ValueEntry(hash, DataType.HASH, -1));
        
        // Save
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        new RdbEncoder().encode(db, dos);
        dos.close();
        
        byte[] rdbData = baos.toByteArray();
        
        // Clear
        db.clearAll();
        assertEquals(0, db.size(0));
        
        // Load
        ByteArrayInputStream bais = new ByteArrayInputStream(rdbData);
        new RdbParser(bais).parse(db);
        
        // Verify
        assertEquals(3, db.size(0));
        
        ValueEntry vS = db.get(0, "s");
        assertEquals(DataType.STRING, vS.type);
        assertArrayEquals("str".getBytes(StandardCharsets.UTF_8), (byte[])vS.getValue());
        
        ValueEntry vL = db.get(0, "l");
        assertEquals(DataType.LIST, vL.type);
        ConcurrentLinkedDeque<String> lLoaded = (ConcurrentLinkedDeque<String>) vL.getValue();
        assertEquals(2, lLoaded.size());
        assertTrue(lLoaded.contains("l1"));
        
        ValueEntry vH = db.get(0, "h");
        assertEquals(DataType.HASH, vH.type);
        ConcurrentHashMap<String, String> hLoaded = (ConcurrentHashMap<String, String>) vH.getValue();
        assertEquals("v1", hLoaded.get("f1"));
    }
}
