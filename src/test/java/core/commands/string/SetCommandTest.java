package core.commands.string;

import core.Carade;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.network.ClientHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SetCommandTest {
    
    static class MockClientHandler extends ClientHandler {
        public String lastError;
        public String lastResponse;
        public int dbIndex = 0;

        @Override
        public void sendError(String msg) {
            this.lastError = msg;
        }
        
        @Override
        public void sendResponse(byte[] respData, String raw) {
            this.lastResponse = raw;
        }
    }

    @BeforeEach
    public void setup() {
        Carade.db = CaradeDatabase.getInstance();
        CaradeDatabase.getInstance().clearAll();
    }

    @Test
    public void testArgumentCombinations() {
        SetCommand cmd = new SetCommand();
        MockClientHandler client = new MockClientHandler();
        
        // SET k v NX EX 10
        List<byte[]> args = new ArrayList<>();
        args.add("SET".getBytes());
        args.add("k".getBytes());
        args.add("v".getBytes());
        args.add("NX".getBytes());
        args.add("EX".getBytes());
        args.add("10".getBytes());
        
        cmd.execute(client, args);
        
        assertTrue(CaradeDatabase.getInstance().exists(0, "k"));
        ValueEntry v = CaradeDatabase.getInstance().get(0, "k");
        assertNotEquals(-1, v.expireAt); // Should have expiry
        
        // SET k v2 NX (Should fail)
        List<byte[]> args2 = new ArrayList<>();
        args2.add("SET".getBytes());
        args2.add("k".getBytes());
        args2.add("v2".getBytes());
        args2.add("NX".getBytes());
        
        client.lastResponse = null;
        cmd.execute(client, args2);
        
        assertEquals("(nil)", client.lastResponse);
        assertEquals("v", new String((byte[])CaradeDatabase.getInstance().get(0, "k").getValue()));
    }
    
    @Test
    public void testInvalidExpiration() {
        SetCommand cmd = new SetCommand();
        MockClientHandler client = new MockClientHandler();
        
        // SET k v EX abc
        List<byte[]> args = new ArrayList<>();
        args.add("SET".getBytes());
        args.add("k".getBytes());
        args.add("v".getBytes());
        args.add("EX".getBytes());
        args.add("abc".getBytes());
        
        cmd.execute(client, args);
        
        assertNotNull(client.lastError);
        assertTrue(client.lastError.contains("integer"));
    }

    @Test
    public void testMutuallyExclusiveOptions() {
        SetCommand cmd = new SetCommand();
        MockClientHandler client = new MockClientHandler();
        
        // SET k v NX XX
        List<byte[]> args = new ArrayList<>();
        args.add("SET".getBytes());
        args.add("k".getBytes());
        args.add("v".getBytes());
        args.add("NX".getBytes());
        args.add("XX".getBytes());
        
        cmd.execute(client, args);
        
        assertNotNull(client.lastError);
        assertTrue(client.lastError.contains("syntax error"));
    }

    @Test
    public void testTimeUnits() {
        SetCommand cmd = new SetCommand();
        MockClientHandler client = new MockClientHandler();
        
        // SET k1 v EX 1
        List<byte[]> args1 = new ArrayList<>();
        args1.add("SET".getBytes());
        args1.add("k1".getBytes());
        args1.add("v".getBytes());
        args1.add("EX".getBytes());
        args1.add("1".getBytes());
        
        long start = System.currentTimeMillis();
        cmd.execute(client, args1);
        ValueEntry v1 = CaradeDatabase.getInstance().get(0, "k1");
        long diff1 = v1.expireAt - start;
        assertTrue(diff1 >= 1000 && diff1 < 2000, "EX should use seconds");
        
        // SET k2 v PX 1000
        List<byte[]> args2 = new ArrayList<>();
        args2.add("SET".getBytes());
        args2.add("k2".getBytes());
        args2.add("v".getBytes());
        args2.add("PX".getBytes());
        args2.add("1000".getBytes());
        
        start = System.currentTimeMillis();
        cmd.execute(client, args2);
        ValueEntry v2 = CaradeDatabase.getInstance().get(0, "k2");
        long diff2 = v2.expireAt - start;
        assertTrue(diff2 >= 1000 && diff2 < 2000, "PX should use milliseconds");
    }

    @Test
    public void testLargeValue() {
        SetCommand cmd = new SetCommand();
        MockClientHandler client = new MockClientHandler();
        
        int size = 10 * 1024 * 1024; // 10MB
        byte[] largeVal = new byte[size];
        
        // SET k large
        List<byte[]> args = new ArrayList<>();
        args.add("SET".getBytes());
        args.add("k_large".getBytes());
        args.add(largeVal);
        
        cmd.execute(client, args);
        
        ValueEntry v = CaradeDatabase.getInstance().get(0, "k_large");
        assertNotNull(v);
        assertEquals(size, ((byte[])v.getValue()).length);
        assertEquals("OK", client.lastResponse);
    }
}
