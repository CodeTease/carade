package core.commands.json;

import core.Carade;
import core.MockClientHandler;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonCommandTest {

    @BeforeEach
    public void setup() {
        if (Carade.db == null) {
            Carade.db = CaradeDatabase.getInstance();
        }
        Carade.db.clearAll();
    }

    @Test
    public void testJsonSetRoot() {
        JsonSetCommand cmd = new JsonSetCommand();
        MockClientHandler client = new MockClientHandler();

        // JSON.SET key $ {"a": 1}
        List<byte[]> args = new ArrayList<>();
        args.add("key".getBytes(StandardCharsets.UTF_8));
        args.add("$".getBytes(StandardCharsets.UTF_8));
        args.add("{\"a\": 1}".getBytes(StandardCharsets.UTF_8));

        cmd.execute(client, args);
        assertEquals("OK", client.lastResponse);

        ValueEntry entry = Carade.db.get(0, "key");
        assertNotNull(entry);
    }

    @Test
    public void testJsonGet() {
        JsonSetCommand setCmd = new JsonSetCommand();
        MockClientHandler client = new MockClientHandler();
        
        List<byte[]> argsSet = new ArrayList<>();
        argsSet.add("doc".getBytes(StandardCharsets.UTF_8));
        argsSet.add("$".getBytes(StandardCharsets.UTF_8));
        argsSet.add("{\"a\": 1, \"b\": {\"c\": 2}}".getBytes(StandardCharsets.UTF_8));
        setCmd.execute(client, argsSet);
        
        JsonGetCommand getCmd = new JsonGetCommand();
        
        // JSON.GET doc $
        List<byte[]> argsGet = new ArrayList<>();
        argsGet.add("doc".getBytes(StandardCharsets.UTF_8));
        argsGet.add("$".getBytes(StandardCharsets.UTF_8));
        
        getCmd.execute(client, argsGet);
        // Expects [{"a":1,"b":{"c":2}}] or similar, depending on implementation
        // RedisJSON returns array for root query usually if path is provided.
        // Assuming current implementation returns the JSON string.
        assertNotNull(client.lastResponse);
        assertTrue(client.lastResponse.contains("\"a\":1") || client.lastResponse.contains("\"a\": 1"));
    }
    
    @Test
    public void testJsonSetNested() {
        JsonSetCommand setCmd = new JsonSetCommand();
        MockClientHandler client = new MockClientHandler();
        
        // Init
        List<byte[]> argsSet = new ArrayList<>();
        argsSet.add("doc".getBytes(StandardCharsets.UTF_8));
        argsSet.add("$".getBytes(StandardCharsets.UTF_8));
        argsSet.add("{\"a\": 1}".getBytes(StandardCharsets.UTF_8));
        setCmd.execute(client, argsSet);
        
        // Update nested: JSON.SET doc $.a 2
        List<byte[]> argsUpdate = new ArrayList<>();
        argsUpdate.add("doc".getBytes(StandardCharsets.UTF_8));
        argsUpdate.add("$.a".getBytes(StandardCharsets.UTF_8));
        argsUpdate.add("2".getBytes(StandardCharsets.UTF_8));
        
        setCmd.execute(client, argsUpdate);
        assertEquals("OK", client.lastResponse);
        
        // Verify
        JsonGetCommand getCmd = new JsonGetCommand();
        List<byte[]> argsGet = new ArrayList<>();
        argsGet.add("doc".getBytes(StandardCharsets.UTF_8));
        
        getCmd.execute(client, argsGet);
        assertNotNull(client.lastResponse);
        assertTrue(client.lastResponse.contains("2"));
    }
}
