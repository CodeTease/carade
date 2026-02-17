package core;

public interface ServerContext {
    // Server
    String getVersion();
    int getPort();
    long getUptime();
    String getOsName();
    String getOsArch();
    String getJavaVersion();
    
    // Clients
    int getActiveConnections();
    
    // Memory
    long getUsedMemory();
    long getMaxMemory();
    
    // Stats
    long getTotalCommandsProcessed();
    long getKeyspaceHits();
    long getKeyspaceMisses();
    
    // Persistence
    boolean isAofEnabled();
    long getLastSaveTime();
    
    // CPU
    int getAvailableProcessors();
    
    // Storage
    int getDbSize();
}
