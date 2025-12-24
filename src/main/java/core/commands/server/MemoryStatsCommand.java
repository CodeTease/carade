package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MemoryStatsCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        List<Object> stats = new ArrayList<>();
        
        Runtime rt = Runtime.getRuntime();
        
        stats.add("peak.allocated".getBytes(StandardCharsets.UTF_8));
        stats.add(rt.totalMemory());
        
        stats.add("total.allocated".getBytes(StandardCharsets.UTF_8));
        stats.add(rt.totalMemory());
        
        stats.add("startup.allocated".getBytes(StandardCharsets.UTF_8));
        stats.add(0); // Dummy
        
        stats.add("replication.backlog".getBytes(StandardCharsets.UTF_8));
        stats.add(1024*1024); // Dummy
        
        stats.add("clients.slaves".getBytes(StandardCharsets.UTF_8));
        stats.add(0); // Dummy
        
        stats.add("clients.normal".getBytes(StandardCharsets.UTF_8));
        stats.add(Carade.activeConnections.get());
        
        stats.add("aof.buffer".getBytes(StandardCharsets.UTF_8));
        stats.add(0); // Dummy
        
        stats.add("lua.caches".getBytes(StandardCharsets.UTF_8));
        stats.add(0);
        
        stats.add("db.0".getBytes(StandardCharsets.UTF_8));
        List<Object> dbStats = new ArrayList<>();
        dbStats.add("overhead.hashtable.main".getBytes(StandardCharsets.UTF_8));
        dbStats.add(Carade.db.size() * 32); // Rough
        dbStats.add("overhead.hashtable.expires".getBytes(StandardCharsets.UTF_8));
        dbStats.add(0);
        stats.add(dbStats);
        
        stats.add("overhead.total".getBytes(StandardCharsets.UTF_8));
        stats.add((rt.totalMemory() - rt.freeMemory()));
        
        stats.add("keys.count".getBytes(StandardCharsets.UTF_8));
        stats.add(Carade.db.size());
        
        stats.add("keys.bytes-per-key".getBytes(StandardCharsets.UTF_8));
        stats.add(Carade.db.size() == 0 ? 0 : (rt.totalMemory() - rt.freeMemory()) / Carade.db.size());
        
        stats.add("dataset.bytes".getBytes(StandardCharsets.UTF_8));
        stats.add(rt.totalMemory() - rt.freeMemory());
        
        stats.add("dataset.percentage".getBytes(StandardCharsets.UTF_8));
        stats.add("50.0".getBytes(StandardCharsets.UTF_8)); // Fake
        
        stats.add("peak.percentage".getBytes(StandardCharsets.UTF_8));
        stats.add("50.0".getBytes(StandardCharsets.UTF_8)); // Fake

        client.sendMixedArray(stats);
    }
}
