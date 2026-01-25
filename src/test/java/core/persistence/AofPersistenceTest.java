package core.persistence;

import core.Carade;
import core.db.CaradeDatabase;
import core.db.DataType;
import core.db.ValueEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class AofPersistenceTest {

    private File aofFile;
    private CommandLogger logger;

    @BeforeEach
    public void setup() throws IOException {
        aofFile = File.createTempFile("carade_test", ".aof");
        aofFile.deleteOnExit();
        
        Carade.db = CaradeDatabase.getInstance();
        CaradeDatabase.getInstance().clearAll();
        
        // Initialize custom logger for this test
        logger = new CommandLogger(aofFile);
        Carade.aofHandler = logger;
    }

    @AfterEach
    public void teardown() {
        if (logger != null) {
            logger.close();
        }
        if (aofFile != null) {
            aofFile.delete();
        }
    }

    @Test
    public void testAofReplay() {
        // Log some commands
        logger.log("SET", "key1", "val1");
        logger.log("RPUSH", "list1", "item1", "item2");
        logger.flush();
        
        // Clear DB
        CaradeDatabase.getInstance().clearAll();
        
        // Replay
        logger.replay(Carade::executeAofCommand);
        
        // Verify
        assertEquals("val1", new String((byte[])CaradeDatabase.getInstance().get(0, "key1").getValue()));
        assertTrue(CaradeDatabase.getInstance().exists(0, "list1"));
    }

    @Test
    public void testTruncatedAof() throws IOException {
        // Generate valid AOF
        logger.log("SET", "k1", "v1");
        logger.log("SET", "k2", "v2");
        logger.flush();
        
        // Append garbage
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(aofFile, true)) {
            fos.write("INCOMPLETE_CMD".getBytes());
        }
        
        CaradeDatabase.getInstance().clearAll();
        
        // Replay
        try {
            logger.replay(Carade::executeAofCommand);
        } catch (Exception e) {
            // Expected to fail on garbage or just log error
        }
        
        // Verify valid data was loaded
        assertEquals("v1", new String((byte[])CaradeDatabase.getInstance().get(0, "k1").getValue()));
        assertEquals("v2", new String((byte[])CaradeDatabase.getInstance().get(0, "k2").getValue()));
    }

    @Test
    public void testAofRewrite() {
        // Populate DB
        CaradeDatabase.getInstance().put("k1", new ValueEntry("v1".getBytes(), DataType.STRING, -1));
        CaradeDatabase.getInstance().put("k2", new ValueEntry("v2".getBytes(), DataType.STRING, -1));
        
        // Log many redundant commands to make file large (simulated)
        for(int i=0; i<100; i++) {
             logger.log("SET", "k1", "v1_old_" + i);
        }
        
        // Trigger Rewrite
        logger.rewrite(CaradeDatabase.getInstance());
        
        // Verify DB is still correct
        assertEquals("v1", new String((byte[])CaradeDatabase.getInstance().get(0, "k1").getValue()));
        
        // Clear DB and Replay from Rewritten AOF
        CaradeDatabase.getInstance().clearAll();
        logger.replay(Carade::executeAofCommand);
        
        assertEquals("v1", new String((byte[])CaradeDatabase.getInstance().get(0, "k1").getValue()));
        assertEquals("v2", new String((byte[])CaradeDatabase.getInstance().get(0, "k2").getValue()));
        
        // Ensure k1 only exists once (implied by correct replay)
    }
}
