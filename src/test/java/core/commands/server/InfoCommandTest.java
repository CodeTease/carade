package core.commands.server;

import core.MockClientHandler;
import core.ServerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class InfoCommandTest {

    private InfoCommand command;
    private MockClientHandler client;

    @BeforeEach
    void setUp() {
        ServerContext context = new MockServerContext();
        command = new InfoCommand(context);
        client = new MockClientHandler();
    }

    @Test
    void testInfoDefault() {
        command.execute(client, Collections.emptyList());
        String response = client.lastResponse;
        
        assertTrue(response.contains("# Server"), "Should contain Server section");
        assertTrue(response.contains("# Memory"), "Should contain Memory section");
        assertTrue(response.contains("# Persistence"), "Should contain Persistence section");
        assertTrue(response.contains("\r\n"), "Should use CRLF");
    }

    @Test
    void testInfoSection() {
        command.execute(client, List.of("memory".getBytes(StandardCharsets.UTF_8)));
        String response = client.lastResponse;
        
        assertTrue(response.contains("# Memory"), "Should contain Memory section");
        assertFalse(response.contains("# Server"), "Should NOT contain Server section");
    }

    @Test
    void testInfoCaseInsensitive() {
        command.execute(client, List.of("MEMORY".getBytes(StandardCharsets.UTF_8)));
        String response = client.lastResponse;
        
        assertTrue(response.contains("# Memory"), "Should contain Memory section");
    }
    
    @Test
    void testDynamicValues() {
        command.execute(client, Collections.emptyList());
        String response = client.lastResponse;
        
        assertTrue(response.contains("carade_version:1.0.0-TEST"), "Should verify version from context");
        assertTrue(response.contains("tcp_port:6379"), "Should verify port from context");
    }

    // Mock Context
    static class MockServerContext implements ServerContext {
        @Override public String getVersion() { return "1.0.0-TEST"; }
        @Override public int getPort() { return 6379; }
        @Override public long getUptime() { return 10000; }
        @Override public String getOsName() { return "TestOS"; }
        @Override public String getOsArch() { return "x64"; }
        @Override public String getJavaVersion() { return "11"; }
        @Override public int getActiveConnections() { return 5; }
        @Override public long getUsedMemory() { return 1024; }
        @Override public long getMaxMemory() { return 2048; }
        @Override public long getTotalCommandsProcessed() { return 100; }
        @Override public long getKeyspaceHits() { return 10; }
        @Override public long getKeyspaceMisses() { return 5; }
        @Override public boolean isAofEnabled() { return true; }
        @Override public long getLastSaveTime() { return 123456789; }
        @Override public int getAvailableProcessors() { return 4; }
        @Override public int getDbSize() { return 50; }
    }
}
