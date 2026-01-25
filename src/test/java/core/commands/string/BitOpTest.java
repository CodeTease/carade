package core.commands.string;

import core.Carade;
import core.commands.string.SetBitCommand;
import core.commands.string.GetBitCommand;
import core.db.CaradeDatabase;
import core.network.ClientHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BitOpTest {

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
    public void testSetBitBoundaries() {
        SetBitCommand setBit = new SetBitCommand();
        GetBitCommand getBit = new GetBitCommand();
        MockClientHandler client = new MockClientHandler();

        // SETBIT key 0 1 -> 1st bit of 1st byte
        setBit.execute(client, makeArgs("SETBIT", "k", "0", "1"));
        assertEquals(0L, client.lastIntegerResponse); // Old value was 0

        // GETBIT key 0
        getBit.execute(client, makeArgs("GETBIT", "k", "0"));
        assertEquals(1L, client.lastIntegerResponse);
        
        // Check array size (implicitly by getting byte[] from DB)
        byte[] val = (byte[]) CaradeDatabase.getInstance().get(0, "k").getValue();
        assertEquals(1, val.length);
        assertEquals((byte)0x80, val[0]); // 10000000

        // SETBIT key 7 1 -> 8th bit of 1st byte
        setBit.execute(client, makeArgs("SETBIT", "k", "7", "1"));
        val = (byte[]) CaradeDatabase.getInstance().get(0, "k").getValue();
        assertEquals(1, val.length);
        assertEquals((byte)0x81, val[0]); // 10000001 (unsigned 129, java byte -127?)
        
        // SETBIT key 8 1 -> 1st bit of 2nd byte
        setBit.execute(client, makeArgs("SETBIT", "k", "8", "1"));
        val = (byte[]) CaradeDatabase.getInstance().get(0, "k").getValue();
        assertEquals(2, val.length);
        assertEquals((byte)0x81, val[0]);
        assertEquals((byte)0x80, val[1]);
        
        // SETBIT key 16 1 -> 1st bit of 3rd byte
        setBit.execute(client, makeArgs("SETBIT", "k", "16", "1"));
        val = (byte[]) CaradeDatabase.getInstance().get(0, "k").getValue();
        assertEquals(3, val.length);
        assertEquals((byte)0x80, val[2]);
        
        // Verify GETBIT works on expanded array
        getBit.execute(client, makeArgs("GETBIT", "k", "16"));
        assertEquals(1L, client.lastIntegerResponse);
        getBit.execute(client, makeArgs("GETBIT", "k", "15")); // should be 0
        assertEquals(0L, client.lastIntegerResponse);
    }
}
