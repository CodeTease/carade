package core.scripting;

import core.Carade;
import core.MockClientHandler;
import core.commands.scripting.EvalCommand;
import core.commands.string.GetCommand;
import core.commands.string.SetCommand;
import core.db.CaradeDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class LuaConcurrencyTest {

    @BeforeEach
    public void setup() {
        if (Carade.db == null) {
            Carade.db = CaradeDatabase.getInstance();
        }
        Carade.db.clearAll();
    }

    @Test
    public void testLuaScriptAtomicity() throws InterruptedException {
        // Prepare data
        SetCommand setCmd = new SetCommand();
        MockClientHandler clientInit = new MockClientHandler();
        List<byte[]> argsSet = new ArrayList<>();
        argsSet.add("SET".getBytes());
        argsSet.add("foo".getBytes());
        argsSet.add("bar".getBytes());
        setCmd.execute(clientInit, argsSet);

        CountDownLatch latchStarted = new CountDownLatch(1);
        CountDownLatch latchDone = new CountDownLatch(1);
        AtomicLong getFinishedTime = new AtomicLong(0);

        // Thread 1: Run a slow Lua script (simulated delay via busy wait in Lua)
        // Note: Thread.sleep in Lua might not be available or reliable, so we use loop
        Thread t1 = new Thread(() -> {
            EvalCommand evalCmd = new EvalCommand();
            MockClientHandler client1 = new MockClientHandler();
            List<byte[]> args = new ArrayList<>();
            args.add("EVAL".getBytes());
            // Busy wait loop ~ 500ms (adjust iterations as needed, Java/Luaj is reasonably fast)
            // 20 million iterations should take noticeable time
            String script = "local i=0; while i < 20000000 do i=i+1 end return 'done'";
            args.add(script.getBytes());
            args.add("0".getBytes());

            // Simulate Server Locking
            Carade.globalRWLock.writeLock().lock();
            try {
                latchStarted.countDown();
                evalCmd.execute(client1, args);
            } finally {
                Carade.globalRWLock.writeLock().unlock();
                latchDone.countDown();
            }
        });

        // Thread 2: Try to GET foo
        Thread t2 = new Thread(() -> {
            try {
                // Wait for T1 to start and acquire lock
                latchStarted.await();
                // Add small delay to ensure T1 is definitely running script
                Thread.sleep(50);
                
                GetCommand getCmd = new GetCommand();
                MockClientHandler client2 = new MockClientHandler();
                List<byte[]> args = new ArrayList<>();
                args.add("GET".getBytes());
                args.add("foo".getBytes());

                // Simulate Server Locking
                Carade.globalRWLock.readLock().lock();
                try {
                    getCmd.execute(client2, args);
                } finally {
                    Carade.globalRWLock.readLock().unlock();
                }
                getFinishedTime.set(System.currentTimeMillis());
                
            } catch (InterruptedException e) {}
        });

        long start = System.currentTimeMillis();
        t1.start();
        t2.start();
        
        t1.join();
        t2.join();
        
        // Validation:
        // T2 should have finished AFTER T1 released the lock
        // So getFinishedTime should be roughly start + 500ms
        assertTrue(getFinishedTime.get() - start >= 400, "GET should have been blocked by Lua script");
    }
}
