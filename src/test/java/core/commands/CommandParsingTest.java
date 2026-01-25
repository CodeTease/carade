package core.commands;

import core.Carade;
import core.commands.string.GetCommand;
import core.commands.string.SetCommand;
import core.db.CaradeDatabase;
import core.network.ClientHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CommandParsingTest {

    static class MockClientHandler extends ClientHandler {
        public String lastError;

        @Override
        public void sendError(String msg) {
            this.lastError = msg;
        }
    }

    @BeforeEach
    public void setup() {
        Carade.db = CaradeDatabase.getInstance();
    }

    private List<byte[]> makeArgs(String... args) {
        List<byte[]> list = new ArrayList<>();
        for (String s : args) {
            list.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Test
    public void testSetCommandMalformed() {
        SetCommand cmd = new SetCommand();
        MockClientHandler client = new MockClientHandler();

        // Missing value: SET k
        cmd.execute(client, makeArgs("SET", "k"));
        assertNotNull(client.lastError);
        assertTrue(client.lastError.contains("usage") || client.lastError.contains("wrong number of arguments"));
    }
    
    @Test
    public void testGetCommandMalformed() {
        GetCommand cmd = new GetCommand();
        MockClientHandler client = new MockClientHandler();
        
        // Missing key: GET
        cmd.execute(client, makeArgs("GET"));
        assertNotNull(client.lastError);
    }
    
    @Test
    public void testGenericWrongType() {
        // This tests logic inside command that parses integers
        SetCommand cmd = new SetCommand();
        MockClientHandler client = new MockClientHandler();
        
        // SET k v EX abc
        cmd.execute(client, makeArgs("SET", "k", "v", "EX", "abc"));
        assertNotNull(client.lastError);
        assertTrue(client.lastError.contains("integer"));
    }
}
