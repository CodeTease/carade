package core.replication;

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

public class ReplicationTest {

    @BeforeEach
    public void setup() {
        // Ensure DB is initialized
        if (Carade.db == null) {
            Carade.db = CaradeDatabase.getInstance();
        }
        Carade.db.clearAll();
    }

    @Test
    public void testReplicaCommandPropagation() {
        // Simulate a Replica receiving a propagated command stream
        // The Replica loop calls Carade.executeAofCommand(parts)
        
        // Command: SET key value
        List<byte[]> parts = new ArrayList<>();
        parts.add("SET".getBytes(StandardCharsets.UTF_8));
        parts.add("key_repl".getBytes(StandardCharsets.UTF_8));
        parts.add("value_repl".getBytes(StandardCharsets.UTF_8));

        Carade.executeAofCommand(parts);

        // Verify the command was applied to the DB
        ValueEntry entry = Carade.db.get(0, "key_repl");
        assertNotNull(entry, "Replica should have applied the SET command");
        assertEquals("value_repl", new String((byte[]) entry.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    public void testReplicaMultipleCommands() {
        // Command 1: RPUSH list a
        List<byte[]> cmd1 = new ArrayList<>();
        cmd1.add("RPUSH".getBytes(StandardCharsets.UTF_8));
        cmd1.add("mylist".getBytes(StandardCharsets.UTF_8));
        cmd1.add("a".getBytes(StandardCharsets.UTF_8));
        Carade.executeAofCommand(cmd1);

        // Command 2: RPUSH list b
        List<byte[]> cmd2 = new ArrayList<>();
        cmd2.add("RPUSH".getBytes(StandardCharsets.UTF_8));
        cmd2.add("mylist".getBytes(StandardCharsets.UTF_8));
        cmd2.add("b".getBytes(StandardCharsets.UTF_8));
        Carade.executeAofCommand(cmd2);

        ValueEntry entry = Carade.db.get(0, "mylist");
        assertNotNull(entry);
    }

    @Test
    public void testPartialResync() {
        // 1. Generate some write traffic to fill backlog
        core.server.WriteSequencer sequencer = core.server.WriteSequencer.getInstance();
        
        // Write: SET p1 v1
        byte[] cmd = new byte[0]; // Logic uses cmd bytes for backlog
        // We need to construct valid RESP bytes for the backlog to make sense if parsed, 
        // but Backlog just stores bytes.
        String setCmd = "*3\r\n$3\r\nSET\r\n$2\r\np1\r\n$2\r\nv1\r\n";
        cmd = setCmd.getBytes(StandardCharsets.UTF_8);
        
        sequencer.executeWrite(() -> {
            Carade.db.put("p1", new ValueEntry("v1".getBytes(), core.db.DataType.STRING, -1));
        }, cmd);
        
        long currentOffset = sequencer.getBacklog().getGlobalOffset();
        assertTrue(currentOffset > 0, "Backlog offset should increase");
        
        // 2. Simulate PSYNC request with a slightly old offset (valid in backlog)
        // Request offset = currentOffset - cmd.length
        long reqOffset = currentOffset - cmd.length;
        
        core.commands.replication.PsyncCommand psync = new core.commands.replication.PsyncCommand();
        MockClientHandler client = new MockClientHandler();
        
        List<byte[]> args = new ArrayList<>();
        args.add("PSYNC".getBytes(StandardCharsets.UTF_8));
        args.add("?".getBytes(StandardCharsets.UTF_8)); // replid
        args.add(String.valueOf(reqOffset).getBytes(StandardCharsets.UTF_8)); // offset
        
        psync.execute(client, args);
        
        // 3. Expect +CONTINUE 
        
        assertNotNull(client.lastResponse);
        assertFalse(client.lastResponse.startsWith("FULLRESYNC"), "Should not be FULLRESYNC");
    }
}
