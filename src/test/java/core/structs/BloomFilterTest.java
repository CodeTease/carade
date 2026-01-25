package core.structs;

import core.Carade;
import core.commands.bloom.BfAddCommand;
import core.commands.bloom.BfExistsCommand;
import core.db.CaradeDatabase;
import core.network.ClientHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BloomFilterTest {

    static class MockClientHandler extends ClientHandler {
        public Long lastIntegerResponse;

        @Override
        public void sendInteger(long value) {
            this.lastIntegerResponse = value;
        }
    }

    @BeforeEach
    public void setup() {
        Carade.db = CaradeDatabase.getInstance();
        CaradeDatabase.getInstance().clearAll();
    }

    private List<byte[]> makeArgs(String... args) {
        List<byte[]> list = new ArrayList<>();
        for (String s : args) {
            list.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Test
    public void testBfAddAndExists() {
        BfAddCommand bfAdd = new BfAddCommand();
        BfExistsCommand bfExists = new BfExistsCommand();
        MockClientHandler client = new MockClientHandler();

        // BF.ADD bf item1
        bfAdd.execute(client, makeArgs("BF.ADD", "bf", "item1"));
        assertEquals(1L, client.lastIntegerResponse); // 1 = added

        // BF.EXISTS bf item1
        bfExists.execute(client, makeArgs("BF.EXISTS", "bf", "item1"));
        assertEquals(1L, client.lastIntegerResponse);

        // BF.EXISTS bf item2 (should not exist)
        bfExists.execute(client, makeArgs("BF.EXISTS", "bf", "item2"));
        assertEquals(0L, client.lastIntegerResponse);

        // BF.ADD bf item1 (duplicate)
        bfAdd.execute(client, makeArgs("BF.ADD", "bf", "item1"));
        assertEquals(0L, client.lastIntegerResponse); // 0 = already exists
    }

    @Test
    public void testFalsePositives() {
        // Since we can't easily configure the BF params via BF.ADD (it uses defaults in current impl or maybe it reserves),
        // let's just check that different items don't collide for a small set.
        // The default implementation uses n=10000, p=0.01.
        
        BfAddCommand bfAdd = new BfAddCommand();
        BfExistsCommand bfExists = new BfExistsCommand();
        MockClientHandler client = new MockClientHandler();

        for (int i = 0; i < 100; i++) {
            bfAdd.execute(client, makeArgs("BF.ADD", "bf_fp", "val" + i));
            assertEquals(1L, client.lastIntegerResponse, "Collision at " + i);
        }

        // Check verification
        for (int i = 0; i < 100; i++) {
            bfExists.execute(client, makeArgs("BF.EXISTS", "bf_fp", "val" + i));
            assertEquals(1L, client.lastIntegerResponse);
        }
        
        // Check non-existent
        bfExists.execute(client, makeArgs("BF.EXISTS", "bf_fp", "impossiblestring12345"));
        assertEquals(0L, client.lastIntegerResponse);
    }
}
