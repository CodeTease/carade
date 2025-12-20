package core.server;

import core.Carade;
import core.persistence.CommandLogger;
import core.replication.ReplicationBacklog;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Global Sequencer.
 * Coordinates all write operations to ensure:
 * 1. DB Order
 * 2. Backlog Order
 * 3. AOF Order
 * are identical.
 */
public class WriteSequencer {
    private static WriteSequencer INSTANCE;
    
    // The Global Lock (we reuse Carade's lock for now, or introduce a new one)
    // Using Carade.globalRWLock ensures compatibility with existing code that might grab it.
    private final ReentrantReadWriteLock lock = Carade.globalRWLock;
    
    private final ReplicationBacklog backlog;
    private final CommandLogger commandLogger;

    private WriteSequencer() {
        // 1MB Backlog for demo
        this.backlog = new ReplicationBacklog(1024 * 1024); 
        this.commandLogger = CommandLogger.getInstance();
    }

    public static synchronized WriteSequencer getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new WriteSequencer();
        }
        return INSTANCE;
    }

    /**
     * Executes a write operation atomically.
     * @param dbOperation The lambda updating the RAM (HashMap)
     * @param commandBytes The raw bytes of the command (already transformed/normalized) for logging
     */
    public void executeWrite(Runnable dbOperation, byte[] commandBytes) {
        lock.writeLock().lock();
        try {
            // 1. Update RAM
            dbOperation.run();
            
            // 2. Append to Replication Backlog
            if (backlog != null && commandBytes != null) {
                backlog.write(commandBytes);
            }
            
            // 3. Write to AOF
            if (commandLogger != null && commandBytes != null) {
                commandLogger.log(commandBytes);
            }

            // 4. Propagate to Replicas
            if (commandBytes != null) {
                core.replication.ReplicationManager.getInstance().propagate(commandBytes);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public ReplicationBacklog getBacklog() {
        return backlog;
    }
}
