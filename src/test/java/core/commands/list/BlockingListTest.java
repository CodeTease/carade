package core.commands.list;

import core.Carade;
import core.db.CaradeDatabase;
import core.network.ClientHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class BlockingListTest {

    static class MockClientHandler extends ClientHandler {
        public List<String> lastArray = new ArrayList<>();
        public boolean arraySent = false;

        @Override
        public void sendArray(List<byte[]> list) {
            this.lastArray.clear();
            for (byte[] o : list) {
                this.lastArray.add(new String(o, StandardCharsets.UTF_8));
            }
            arraySent = true;
        }
        
        public Runnable capturedTimeoutTask;
        @Override
        public void scheduleTimeout(Runnable task, long delayMs) {
            this.capturedTimeoutTask = task;
        }
        
        @Override
        public void sendNull() {
             this.arraySent = true;
             this.lastArray.clear();
             this.lastArray.add("(nil)");
        }
    }

    @BeforeEach
    public void setup() {
        Carade.db = CaradeDatabase.getInstance();
        CaradeDatabase.getInstance().clearAll();
        Carade.blockingRegistry.clear();
    }

    @AfterEach
    public void teardown() {
        Carade.blockingRegistry.clear();
    }

    private List<byte[]> makeArgs(String... args) {
        List<byte[]> list = new ArrayList<>();
        for (String s : args) {
            list.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Test
    public void testBlockingPop() {
        BlPopCommand blPop = new BlPopCommand();
        RPushCommand rPush = new RPushCommand();
        
        MockClientHandler clientA = new MockClientHandler(); // Blocker
        MockClientHandler clientB = new MockClientHandler(); // Pusher

        // Client A: BLPOP list 0 (wait forever)
        // This runs on main thread but async attaches listener.
        blPop.execute(clientA, makeArgs("BLPOP", "mylist", "0"));

        // Verify A is blocked (no response yet)
        assertFalse(clientA.arraySent, "Client A should be blocked");
        assertTrue(Carade.blockingRegistry.containsKey("mylist"), "Blocking registry should have key");
        assertFalse(Carade.blockingRegistry.get("mylist").isEmpty(), "Blocking queue should not be empty");

        // Client B: RPUSH mylist "hello"
        // This triggers checkBlockers -> completes future -> calls clientA.sendArray
        rPush.execute(clientB, makeArgs("RPUSH", "mylist", "hello"));

        // Verify A received response
        assertTrue(clientA.arraySent, "Client A should have received response");
        assertEquals(2, clientA.lastArray.size());
        assertEquals("mylist", clientA.lastArray.get(0));
        assertEquals("hello", clientA.lastArray.get(1));
        
        // Verify list is empty (since A popped it)
        assertFalse(CaradeDatabase.getInstance().exists(0, "mylist"));
        
        // Verify Blocking registry is clean (request removed)
        // checkBlockers removes the request from the queue
        assertTrue(Carade.blockingRegistry.get("mylist").isEmpty());
    }

    @Test
    public void testBlockingTimeout() {
        BlPopCommand blPop = new BlPopCommand();
        MockClientHandler client = new MockClientHandler();

        // BLPOP list 0.1 (100ms timeout)
        blPop.execute(client, makeArgs("BLPOP", "listTimeout", "0.1"));
        
        assertFalse(client.arraySent);
        assertTrue(Carade.blockingRegistry.containsKey("listTimeout"));
        assertNotNull(client.capturedTimeoutTask, "Timeout task should be scheduled");

        // Simulate timeout triggering
        client.capturedTimeoutTask.run();

        // Verify response is nil
        assertTrue(client.arraySent);
        assertEquals(1, client.lastArray.size());
        assertEquals("(nil)", client.lastArray.get(0));
    }
}
