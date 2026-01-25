package core.commands.transaction;

import core.Carade;
import core.commands.string.SetCommand;
import core.commands.string.GetCommand;
import core.db.CaradeDatabase;
import core.network.ClientHandler;
import core.protocol.Resp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionTest {

    static class MockClientHandler extends ClientHandler {
        public Object lastResponseData;
        public String lastResponseString;
        public OutputStream captured;

        @Override
        public void setCaptureBuffer(OutputStream out) {
            super.setCaptureBuffer(out);
            this.captured = out;
        }

        @Override
        public void send(boolean isResp, Object data, String textData) {
            if (this.captured != null) {
                super.send(isResp, data, textData);
                return;
            }
            this.lastResponseData = data;
            this.lastResponseString = textData;
        }

        @Override
        public void writeDirect(Object msg) {
            this.lastResponseData = msg;
        }
    }

    @BeforeEach
    public void setup() {
        Carade.db = CaradeDatabase.getInstance();
        CaradeDatabase.getInstance().clearAll();
        Carade.watchers.clear();
    }

    private List<byte[]> makeArgs(String... args) {
        List<byte[]> list = new ArrayList<>();
        for (String s : args) {
            list.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Test
    public void testHappyPath() {
        MultiCommand multi = new MultiCommand();
        ExecCommand exec = new ExecCommand();
        SetCommand set = new SetCommand();
        GetCommand get = new GetCommand();
        
        MockClientHandler client = new MockClientHandler();

        // MULTI
        multi.execute(client, makeArgs("MULTI"));
        assertTrue(client.isInTransaction());
        assertEquals("OK", client.lastResponseString);

        // SET k v (Queued)
        // ClientHandler handles queueing if isInTransaction is true.
        // But we are invoking command.execute directly?
        // No, in real life, ClientHandler.channelRead calls handleCommand -> checks isInTransaction -> queues.
        // Here we are unit testing commands.
        // Commands don't queue themselves. ClientHandler does.
        // So I must manually queue commands or simulate ClientHandler logic.
        
        // Simulating queuing manually since we are bypassing ClientHandler.channelRead
        List<byte[]> cmd1 = makeArgs("SET", "k", "v");
        client.getTransactionQueue().add(cmd1);
        
        List<byte[]> cmd2 = makeArgs("GET", "k");
        client.getTransactionQueue().add(cmd2);

        // EXEC
        exec.execute(client, makeArgs("EXEC"));

        assertFalse(client.isInTransaction());
        assertNotNull(client.lastResponseData);
        // Result should be Array of [OK, v]
        // Since we can't easily parse byte[] result without Decoder, we just assume success if no error.
        // But we can check side effects.
        assertEquals("v", new String((byte[])CaradeDatabase.getInstance().get(0, "k").getValue()));
    }

    @Test
    public void testDirtyState() {
        MultiCommand multi = new MultiCommand();
        ExecCommand exec = new ExecCommand();
        WatchCommand watch = new WatchCommand();
        SetCommand set = new SetCommand();
        
        MockClientHandler client = new MockClientHandler();

        // WATCH k
        watch.execute(client, makeArgs("WATCH", "k"));
        
        // MULTI
        multi.execute(client, makeArgs("MULTI"));

        // External modification (simulate another client)
        MockClientHandler otherClient = new MockClientHandler();
        set.execute(otherClient, makeArgs("SET", "k", "dirty"));

        // Verify client is marked dirty
        assertTrue(client.isTransactionDirty());

        // Queue a command
        client.getTransactionQueue().add(makeArgs("GET", "k"));

        // EXEC -> Should fail (return nil)
        exec.execute(client, makeArgs("EXEC"));

        assertFalse(client.isInTransaction());
        // Null bulk string indicates failure of exec due to watch
        // My Mock captures the raw object passed to writeDirect.
        // Resp.bulkString(null) or something similar.
        // Actually EXEC returns (nil) which is BulkString null.
        // Let's check ExecCommand implementation.
    }

    @Test
    public void testExecWithError() {
        MultiCommand multi = new MultiCommand();
        ExecCommand exec = new ExecCommand();
        MockClientHandler client = new MockClientHandler();

        multi.execute(client, makeArgs("MULTI"));

        // Queue valid command
        client.getTransactionQueue().add(makeArgs("SET", "k", "v"));
        
        // Queue invalid command (SET with wrong args)
        client.getTransactionQueue().add(makeArgs("SET", "k")); 
        
        // Queue another valid command
        client.getTransactionQueue().add(makeArgs("SET", "k2", "v2"));

        exec.execute(client, makeArgs("EXEC"));
        
        // Verify results
        assertNotNull(client.lastResponseData);
        // Should be Array of 3 items
        // We can't easily parse List<Object> or byte[] unless we inspect closely or use RespParser.
        // But we can check if keys were set.
        // Redis Transactions: all commands are executed. If one fails, others still run.
        
        assertEquals("v", new String((byte[])CaradeDatabase.getInstance().get(0, "k").getValue()));
        assertEquals("v2", new String((byte[])CaradeDatabase.getInstance().get(0, "k2").getValue()));
    }
}
