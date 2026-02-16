package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.lang.management.ManagementFactory;
import java.util.List;

public class InfoCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        StringBuilder info = new StringBuilder();
        info.append("# Server\n");
        info.append("carade_version:0.3.4\n");
        info.append("tcp_port:").append(Carade.config.port).append("\n");
        info.append("uptime_in_seconds:").append(ManagementFactory.getRuntimeMXBean().getUptime() / 1000).append("\n");
        info.append("\n# Clients\n");
        info.append("connected_clients:").append(Carade.activeConnections.get()).append("\n");
        info.append("\n# Memory\n");
        info.append("used_memory:").append(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).append("\n");
        info.append("maxmemory:").append(Carade.config.maxMemory).append("\n");
        info.append("\n# Stats\n");
        info.append("total_commands_processed:").append(Carade.totalCommands.get()).append("\n");
        info.append("keyspace_hits:").append(Carade.keyspaceHits.get()).append("\n");
        info.append("keyspace_misses:").append(Carade.keyspaceMisses.get()).append("\n");
        info.append("\n# Persistence\n");
        info.append("aof_enabled:1\n");
        
        client.sendBulkString(info.toString());
    }
}
