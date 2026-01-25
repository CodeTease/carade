package core.commands.generic;

import core.Carade;
import core.Config;
import core.MockClientHandler;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.db.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class MigrateCommandTest {

    private MockClientHandler client;

    @BeforeEach
    public void setUp() {
        Config config = new Config();
        // Initialize DB with dummy config and null logger
        Carade.db = new CaradeDatabase(config, null);
        client = new MockClientHandler();
    }

    @Test
    public void testMigrateBlockedLocalhost() {
        // Setup key
        String key = "testKey";
        Carade.db.put(key, new ValueEntry("value".getBytes(StandardCharsets.UTF_8), DataType.STRING, -1));

        // Prepare arguments: MIGRATE 127.0.0.1 6379 testKey 0 1000
        List<byte[]> args = Arrays.asList(
            "MIGRATE".getBytes(StandardCharsets.UTF_8),
            "127.0.0.1".getBytes(StandardCharsets.UTF_8),
            "6379".getBytes(StandardCharsets.UTF_8),
            key.getBytes(StandardCharsets.UTF_8),
            "0".getBytes(StandardCharsets.UTF_8), // DB
            "1000".getBytes(StandardCharsets.UTF_8) // Timeout
        );

        MigrateCommand cmd = new MigrateCommand();
        cmd.execute(client, args);

        assertNotNull(client.lastResponse);
        // Expect security error
        assertTrue(client.lastResponse.contains("Security: Destination address is not allowed"), "Expected security error, got: " + client.lastResponse);
    }
    
    @Test
    public void testMigrateBlockedPrivateIP() {
        // Setup key
        String key = "testKey2";
        Carade.db.put(key, new ValueEntry("value".getBytes(StandardCharsets.UTF_8), DataType.STRING, -1));

        // Prepare arguments: MIGRATE 192.168.1.1 6379 testKey 0 1000
        List<byte[]> args = Arrays.asList(
            "MIGRATE".getBytes(StandardCharsets.UTF_8),
            "192.168.1.1".getBytes(StandardCharsets.UTF_8),
            "6379".getBytes(StandardCharsets.UTF_8),
            key.getBytes(StandardCharsets.UTF_8),
            "0".getBytes(StandardCharsets.UTF_8), // DB
            "1000".getBytes(StandardCharsets.UTF_8) // Timeout
        );

        MigrateCommand cmd = new MigrateCommand();
        cmd.execute(client, args);

        assertNotNull(client.lastResponse);
        // Expect security error
        assertTrue(client.lastResponse.contains("Security: Destination address is not allowed"), "Expected security error, got: " + client.lastResponse);
    }
}
