package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

public class UnlinkCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: UNLINK key");
            return;
        }
        
        String unlinkKey = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] unlinkRet = {0};
        
        client.executeWrite(() -> {
            ValueEntry prev = Carade.db.remove(client.getDbIndex(), unlinkKey);
            if (prev != null) {
                Carade.notifyWatchers(unlinkKey);
                unlinkRet[0] = 1;
                
                // Async cleanup if the value is large or complex
                final Object val = prev.getValue();
                if (val instanceof Collection || val instanceof Map || val instanceof CaradeZSet) {
                    ForkJoinPool.commonPool().submit(() -> {
                        try {
                            if (val instanceof Collection) ((Collection<?>)val).clear();
                            else if (val instanceof Map) ((Map<?,?>)val).clear();
                            else if (val instanceof CaradeZSet) {
                                 CaradeZSet z = (CaradeZSet) val;
                                 z.scores.clear();
                                 z.sorted.clear();
                            }
                        } catch (Exception e) {} // Ignore
                    });
                }
            }
        }, "DEL", unlinkKey); // Log as DEL for compatibility
        
        client.sendInteger(unlinkRet[0]);
    }
}
