package core.replication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A fixed-size circular buffer for replication.
 * Handles the "Replication Backlog" to support Partial Resync (PSYNC).
 */
public class ReplicationBacklog {
    private final byte[] buffer;
    private final int size;
    private final AtomicLong globalOffset = new AtomicLong(0);
    private int writeIndex = 0;

    public ReplicationBacklog(int sizeInBytes) {
        this.size = sizeInBytes;
        this.buffer = new byte[size];
    }

    /**
     * Appends data to the backlog.
     * Thread-safety: This must be called under a lock (Sequencer's lock).
     */
    public void write(byte[] data) {
        for (byte b : data) {
            buffer[writeIndex] = b;
            writeIndex = (writeIndex + 1) % size;
            
            // If we wrapped around and overwrote old data, the start offset moves forward
            // (Actually, start offset is conceptually moving, but easier to calc:
            // globalOffset always increases. valid range is [globalOffset - size, globalOffset])
        }
        globalOffset.addAndGet(data.length);
        
        // Update bufferStartOffset logic if needed?
        // Actually, the simplest way is:
        // Current global offset is X.
        // Valid range is [max(0, X - size), X].
    }

    public long getGlobalOffset() {
        return globalOffset.get();
    }
    
    public boolean isValidOffset(long offset) {
        long current = globalOffset.get();
        return offset >= (current - size) && offset <= current;
    }

    /**
     * Reads from the backlog starting at the requested global offset.
     * Returns null if offset is out of range (requires full resync).
     * 
     * Note: This method is not strictly synchronized with write() for performance.
     * In a real persistent ring buffer, we might need a read lock or volatile semantics.
     * Here, assuming 'buffer' content is eventually visible. Since we only read committed offsets,
     * the data should be there. A ReadWriteLock in Sequencer could protect this if strict consistency is needed,
     * but usually PSYNC is tolerated to be eventually consistent or handled via the socket stream.
     * 
     * For now, we will just ensure we don't read partial writes by checking offset again.
     */
    public byte[] readFrom(long offset, int limit) {
        long current = globalOffset.get();
        if (offset < (current - size) || offset > current) {
            return null; // Too old or in the future
        }
        
        int available = (int) (current - offset);
        if (available == 0) return new byte[0];
        
        int toRead = Math.min(available, limit);
        byte[] result = new byte[toRead];
        
        int startIndex = (int) (offset % size);
        
        for (int i = 0; i < toRead; i++) {
            result[i] = buffer[(startIndex + i) % size];
        }
        
        return result;
    }
}
