package core.network;

import core.Carade;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ClientBufferTest {

    @Test
    public void testSlowClientSimulation() {
        ClientHandler client = new ClientHandler();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        client.setCaptureBuffer(buffer);
        
        // Pump data: Simulate sending 10MB of data to a client that "buffers" it (in memory)
        // Since we are using captureBuffer, we are just testing memory usage and stability
        
        int chunkSize = 1024 * 1024; // 1MB
        byte[] chunk = new byte[chunkSize];
        List<byte[]> largeData = new ArrayList<>();
        largeData.add(chunk);
        
        long start = System.currentTimeMillis();
        
        try {
            for (int i = 0; i < 10; i++) { // 10MB total
                client.send(true, chunk, null); // Simulate sending byte array
            }
        } catch (OutOfMemoryError e) {
            fail("Server ran out of memory handling client buffer");
        }
        
        assertEquals(10 * 1024 * 1024, buffer.size());
        
        // Currently, Carade doesn't implement client-output-buffer-limit, 
        // so we don't expect the client to be disconnected.
        // If it were implemented, we would assert client.close() was called or similar.
        
        // This test serves as a benchmark/verification that pumping data works.
        assertTrue(true);
    }
}
