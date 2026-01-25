package core.scripting;

import core.Carade;
import core.commands.scripting.EvalCommand;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.db.DataType;
import core.network.ClientHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LuaScriptingTest {

    static class MockClientHandler extends ClientHandler {
        public Object lastResponseData;
        public String lastResponseString;
        public List<Object> lastArray = new ArrayList<>();
        public java.io.OutputStream captured;

        @Override
        public void setCaptureBuffer(java.io.OutputStream out) {
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
        public void sendInteger(long i) {
            if (this.captured != null) { super.sendInteger(i); return; }
            this.lastResponseData = i;
            this.lastResponseString = "(integer) " + i;
        }

        @Override
        public void sendBulkString(String s) {
            if (this.captured != null) { super.sendBulkString(s); return; }
            this.lastResponseString = s;
            // Also store data for consistency if needed
            this.lastResponseData = s != null ? s.getBytes(StandardCharsets.UTF_8) : null;
        }
        
        @Override
        public void sendArray(List<byte[]> list) {
             if (this.captured != null) { super.sendArray(list); return; }
             this.lastArray.clear();
             for(byte[] b : list) this.lastArray.add(new String(b, StandardCharsets.UTF_8));
        }
        
        @Override
        public void sendMixedArray(List<Object> list) {
             if (this.captured != null) { super.sendMixedArray(list); return; }
             this.lastArray.clear();
             this.lastArray.addAll(list);
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
    public void testReturnValues() {
        EvalCommand eval = new EvalCommand();
        MockClientHandler client = new MockClientHandler();

        // return 10
        eval.execute(client, makeArgs("EVAL", "return 10", "0"));
        assertEquals(10L, client.lastResponseData);

        // return "hello"
        eval.execute(client, makeArgs("EVAL", "return 'hello'", "0"));
        // Lua returns string as byte[] (Bulk String)
        assertNotNull(client.lastResponseData);
        assertTrue(client.lastResponseData instanceof byte[]);
        String resp = new String((byte[]) client.lastResponseData, StandardCharsets.UTF_8);
        assertTrue(resp.contains("hello"));
        assertTrue(resp.startsWith("$5\r\nhello"));
        
        // return {1, 2}
        // EVAL returns mixed array usually
        eval.execute(client, makeArgs("EVAL", "return {1, 2}", "0"));
        assertFalse(client.lastArray.isEmpty());
        // Lua table {1, 2} converts to list [1, 2] (Longs)
        assertEquals(2, client.lastArray.size());
        assertEquals(1L, client.lastArray.get(0));
        assertEquals(2L, client.lastArray.get(1));
    }

    @Test
    public void testRedisCall() {
        EvalCommand eval = new EvalCommand();
        MockClientHandler client = new MockClientHandler();

        // Setup data
        CaradeDatabase.getInstance().put("mykey", new ValueEntry("myval".getBytes(), DataType.STRING, -1));

        // EVAL "return redis.call('GET', KEYS[1])" 1 mykey
        eval.execute(client, makeArgs("EVAL", "return redis.call('GET', KEYS[1])", "1", "mykey"));
        
        // result is byte[] "myval" -> RESP encoded
        assertNotNull(client.lastResponseData);
        assertTrue(client.lastResponseData instanceof byte[]);
        String resp = new String((byte[]) client.lastResponseData, StandardCharsets.UTF_8);
        assertTrue(resp.contains("myval"));
    }

    @Test
    public void testInfiniteLoopScript() throws InterruptedException {
        EvalCommand eval = new EvalCommand();
        MockClientHandler client = new MockClientHandler();

        // Run infinite script in a separate thread
        Thread scriptThread = new Thread(() -> {
            eval.execute(client, makeArgs("EVAL", "while true do end", "0"));
        });
        
        scriptThread.start();
        Thread.sleep(200); // Give it time to start and loop
        
        // Kill it
        core.scripting.ScriptManager.getInstance().killScript();
        
        scriptThread.join(2000); // Wait for termination
        
        // Verify response is error
        assertNotNull(client.lastResponseString);
        assertTrue(client.lastResponseString.contains("Script killed"), "Should report script killed. Got: " + client.lastResponseString);
    }

    @Test
    public void testSandboxing() {
        EvalCommand eval = new EvalCommand();
        MockClientHandler client = new MockClientHandler();
        
        // Try to access java System directly or via luajava
        // Note: standard JsePlatform puts 'luajava' in globals.
        eval.execute(client, makeArgs("EVAL", "return luajava.bindClass('java.lang.System').exit(0)", "0"));
        
        // If successful, the JVM exits and test aborts! We assume it might fail or return error.
        // We can test something safer like property access.
        eval.execute(client, makeArgs("EVAL", "return luajava.bindClass('java.lang.System').getProperty('java.version')", "0"));
        
        // If it returns a version string, it's NOT sandboxed.
        // We expect error.
        
        boolean isError = client.lastResponseString != null && 
            (client.lastResponseString.contains("ERR") || client.lastResponseString.contains("nil"));
        
        // If we got a version string, fail.
        if (!isError && client.lastResponseData != null) {
             String resp = new String((byte[])client.lastResponseData, StandardCharsets.UTF_8);
             assertFalse(resp.contains("."), "Should not return java version: " + resp);
        }
        
        // Check os.execute
        client.lastResponseString = null;
        eval.execute(client, makeArgs("EVAL", "return os.execute('ls')", "0"));
        assertTrue(client.lastResponseString.contains("ERR") || client.lastResponseString.contains("attempt to call field 'execute'") || client.lastResponseString.contains("attempt to index global 'os'"),
            "Should not allow os.execute. Got: " + client.lastResponseString);
            
        // Check io.open
        client.lastResponseString = null;
        eval.execute(client, makeArgs("EVAL", "return io.open('pom.xml')", "0"));
        assertTrue(client.lastResponseString.contains("ERR") || client.lastResponseString.contains("attempt to call field 'open'") || client.lastResponseString.contains("attempt to index global 'io'"),
            "Should not allow io.open. Got: " + client.lastResponseString);
    }
}
