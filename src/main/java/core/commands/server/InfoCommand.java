package core.commands.server;

import core.ServerContext;
import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class InfoCommand implements Command {
    private final ServerContext context;

    public InfoCommand(ServerContext context) {
        this.context = context;
    }

    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        String section = "all";
        if (args != null && !args.isEmpty()) {
            section = new String(args.get(0), StandardCharsets.UTF_8).toLowerCase();
        }

        StringBuilder info = new StringBuilder();
        boolean all = section.equals("all") || section.equals("default");

        if (all || section.equals("server")) appendServer(info);
        if (all || section.equals("clients")) appendClients(info);
        if (all || section.equals("memory")) appendMemory(info);
        if (all || section.equals("persistence")) appendPersistence(info);
        if (all || section.equals("stats")) appendStats(info);
        if (all || section.equals("cpu")) appendCpu(info);
        if (all || section.equals("keyspace")) appendKeyspace(info);

        client.sendBulkString(info.toString());
    }

    private void appendServer(StringBuilder info) {
        info.append("# Server\r\n");
        info.append("carade_version:").append(context.getVersion()).append("\r\n");
        info.append("os:").append(context.getOsName()).append(" ").append(context.getOsArch()).append("\r\n");
        info.append("java_version:").append(context.getJavaVersion()).append("\r\n");
        info.append("tcp_port:").append(context.getPort()).append("\r\n");
        info.append("uptime_in_seconds:").append(context.getUptime() / 1000).append("\r\n");
        info.append("\r\n");
    }

    private void appendClients(StringBuilder info) {
        info.append("# Clients\r\n");
        info.append("connected_clients:").append(context.getActiveConnections()).append("\r\n");
        info.append("\r\n");
    }

    private void appendMemory(StringBuilder info) {
        info.append("# Memory\r\n");
        info.append("used_memory:").append(context.getUsedMemory()).append("\r\n");
        info.append("maxmemory:").append(context.getMaxMemory()).append("\r\n");
        info.append("\r\n");
    }

    private void appendPersistence(StringBuilder info) {
        info.append("# Persistence\r\n");
        info.append("aof_enabled:").append(context.isAofEnabled() ? 1 : 0).append("\r\n");
        info.append("rdb_last_save_time:").append(context.getLastSaveTime()).append("\r\n");
        info.append("\r\n");
    }

    private void appendStats(StringBuilder info) {
        info.append("# Stats\r\n");
        info.append("total_commands_processed:").append(context.getTotalCommandsProcessed()).append("\r\n");
        info.append("keyspace_hits:").append(context.getKeyspaceHits()).append("\r\n");
        info.append("keyspace_misses:").append(context.getKeyspaceMisses()).append("\r\n");
        info.append("\r\n");
    }

    private void appendCpu(StringBuilder info) {
        info.append("# CPU\r\n");
        info.append("used_cpu_sys_children:0.00\r\n"); // Placeholder or implement if possible
        info.append("used_cpu_user_children:0.00\r\n");
        info.append("available_processors:").append(context.getAvailableProcessors()).append("\r\n");
        info.append("\r\n");
    }
    
    private void appendKeyspace(StringBuilder info) {
        if (context.getDbSize() > 0) {
            info.append("# Keyspace\r\n");
            // Assuming DB 0 for now as Carade seems single-DB focused in context or we iterate?
            // Carade supports multiple DBs but context.getDbSize() probably returns total or current?
            // CaradeServerContext implemented getDbSize() as db.size() which is total keys.
            // Standard Redis format: db0:keys=X,expires=Y,avg_ttl=Z
            // We'll just output generic info for now as per requirements.
            info.append("db0:keys=").append(context.getDbSize()).append(",expires=0,avg_ttl=0\r\n");
            info.append("\r\n");
        }
    }
}
