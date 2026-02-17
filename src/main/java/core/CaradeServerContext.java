package core;

import java.lang.management.ManagementFactory;

public class CaradeServerContext implements ServerContext {

    @Override
    public String getVersion() {
        return Carade.config.version;
    }

    @Override
    public int getPort() {
        return Carade.config.port;
    }

    @Override
    public long getUptime() {
        return ManagementFactory.getRuntimeMXBean().getUptime();
    }
    
    @Override
    public String getOsName() {
        return System.getProperty("os.name");
    }

    @Override
    public String getOsArch() {
        return System.getProperty("os.arch");
    }

    @Override
    public String getJavaVersion() {
        return System.getProperty("java.version");
    }

    @Override
    public int getActiveConnections() {
        return Carade.activeConnections.get();
    }

    @Override
    public long getUsedMemory() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    @Override
    public long getMaxMemory() {
        return Carade.config.maxMemory;
    }

    @Override
    public long getTotalCommandsProcessed() {
        return Carade.totalCommands.get();
    }

    @Override
    public long getKeyspaceHits() {
        return Carade.keyspaceHits.get();
    }

    @Override
    public long getKeyspaceMisses() {
        return Carade.keyspaceMisses.get();
    }

    @Override
    public boolean isAofEnabled() {
        return Carade.aofHandler != null;
    }

    @Override
    public long getLastSaveTime() {
        return Carade.lastSaveTime;
    }

    @Override
    public int getAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }
    
    @Override
    public int getDbSize() {
        return Carade.db.size();
    }
}
