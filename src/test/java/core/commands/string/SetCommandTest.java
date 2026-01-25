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
}
