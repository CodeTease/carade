package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BzPopMaxCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'bzpopmax' command");
            return;
        }

        double timeout = 0;
        try {
            timeout = Double.parseDouble(new String(args.get(args.size() - 1), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR timeout is not a float or out of range");
            return;
        }

        List<String> keys = new ArrayList<>();
        for (int i = 1; i < args.size() - 1; i++) {
            keys.add(new String(args.get(i), StandardCharsets.UTF_8));
        }

        boolean served = false;
        // Try to pop immediately
        for (String k : keys) {
            ValueEntry entry = Carade.db.get(client.getDbIndex(), k);
            if (entry != null && entry.type == DataType.ZSET) {
                CaradeZSet zset = (CaradeZSet) entry.getValue();
                if (zset.size() > 0) {
                     final String finalKey = k;
                     final ZNode[] nodeRef = {null};
                     
                     client.executeWrite(() -> {
                         ValueEntry e = Carade.db.get(client.getDbIndex(), finalKey);
                         if (e != null && e.type == DataType.ZSET) {
                             CaradeZSet zs = (CaradeZSet) e.getValue();
                             if (zs.size() > 0) {
                                 List<ZNode> nodes = zs.popMax(1);
                                 if (!nodes.isEmpty()) {
                                     nodeRef[0] = nodes.get(0);
                                     if (zs.size() == 0) Carade.db.remove(client.getDbIndex(), finalKey);
                                     Carade.notifyWatchers(finalKey);
                                 }
                             }
                         }
                     }, "ZPOPMAX", finalKey);
                     
                     if (nodeRef[0] != null) {
                         if (client.isResp()) {
                             client.send(true, Resp.array(Arrays.asList(
                                 finalKey.getBytes(StandardCharsets.UTF_8),
                                 nodeRef[0].member.getBytes(StandardCharsets.UTF_8),
                                 String.valueOf(nodeRef[0].score).getBytes(StandardCharsets.UTF_8)
                             )), null);
                         } else {
                              client.send(false, null, "1) \"" + finalKey + "\"\n2) \"" + nodeRef[0].member + "\"\n3) \"" + nodeRef[0].score + "\"");
                         }
                         served = true;
                         break;
                     }
                }
            }
        }

        if (!served) {
            // Block
            Carade.BlockingRequest req = new Carade.BlockingRequest(client, false, client.getDbIndex(), DataType.ZSET);
            for (String k : keys) {
                Carade.blockingRegistry.computeIfAbsent(k, x -> new ConcurrentLinkedQueue<>()).add(req);
            }
            
            req.future.whenComplete((result, ex) -> {
                 if (ex != null) {
                     client.sendNull();
                 } else {
                     client.sendArray(result);
                 }
            });
            
            if (timeout > 0) {
                client.scheduleTimeout(req, (long)(timeout * 1000));
            }
        }
    }
}
