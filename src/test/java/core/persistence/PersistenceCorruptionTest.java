package core.persistence;

import core.Carade;
import core.db.CaradeDatabase;
import core.persistence.rdb.RdbParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PersistenceCorruptionTest {

    @BeforeEach
    public void setup() {
        if (Carade.db == null) {
            Carade.db = CaradeDatabase.getInstance();
        }
        Carade.db.clearAll();
    }

    @Test
    public void testTruncatedRdb() {
        // Valid header "REDIS" + Version "0009"
        byte[] validHeader = "REDIS0009".getBytes(StandardCharsets.US_ASCII);
        
        // Truncate it
        byte[] truncated = new byte[validHeader.length - 1];
        System.arraycopy(validHeader, 0, truncated, 0, truncated.length);
        
        ByteArrayInputStream in = new ByteArrayInputStream(truncated);
        RdbParser parser = new RdbParser(in);
        
        assertThrows(IOException.class, () -> {
            parser.parse(Carade.db);
        });
    }

    @Test
    public void testCorruptedAofLine() {
        // AOF Replay uses Carade.executeAofCommand(List<byte[]> parts)
        // If parts is empty or contains garbage that CommandRegistry doesn't know, it should just log error and continue
        
        List<byte[]> garbage = new ArrayList<>();
        garbage.add("NONEXISTENTCMD".getBytes(StandardCharsets.UTF_8));
        garbage.add("args".getBytes(StandardCharsets.UTF_8));
        
        // Should not throw exception
        assertDoesNotThrow(() -> {
            Carade.executeAofCommand(garbage);
        });
        
        // Try passing a known command with malformed args that might cause internal error
        // e.g. SET without value (requires 3 parts)
        List<byte[]> malformedSet = new ArrayList<>();
        malformedSet.add("SET".getBytes(StandardCharsets.UTF_8));
        malformedSet.add("key".getBytes(StandardCharsets.UTF_8));
        
        // This might cause IndexOutOfBounds in SetCommand or be handled
        // Carade.executeAofCommand catches Exception and logs it
        assertDoesNotThrow(() -> {
            Carade.executeAofCommand(malformedSet);
        });
    }
}
