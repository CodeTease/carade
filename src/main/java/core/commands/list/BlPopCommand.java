package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.util.List;

public class BlPopCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: BLPOP key [key ...] timeout");
            return;
        }

        try {
            double timeout = Double.parseDouble(new String(args.get(args.size()-1), StandardCharsets.UTF_8));
            List<String> keys = new ArrayList<>();
            for(int i=1; i<args.size()-1; i++) keys.add(new String(args.get(i), StandardCharsets.UTF_8));
            
            boolean served = false;
            for (String k : keys) {
                ValueEntry entry = Carade.db.get(client.getDbIndex(), k);
                if (entry != null && entry.type == DataType.LIST) {
                    ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.getValue();
                    if (!list.isEmpty()) {
                        final String[] valRef = {null};
                        final String finalKey = k;
                        
                        client.executeWrite(() -> {
                            ValueEntry e = Carade.db.get(client.getDbIndex(), finalKey);
                            if (e != null && e.type == DataType.LIST) {
                                ConcurrentLinkedDeque<String> l = (ConcurrentLinkedDeque<String>) e.getValue();
                                valRef[0] = l.pollFirst();
                                
                                if (valRef[0] != null) {
                                    if (l.isEmpty()) Carade.db.remove(client.getDbIndex(), finalKey);
                                    Carade.notifyWatchers(finalKey);
                                }
                            }
                        }, "LPOP", new Object[]{k});
                        
                        if (valRef[0] != null) {
                             List<byte[]> resp = new ArrayList<>();
                             resp.add(k.getBytes(StandardCharsets.UTF_8));
                             resp.add(valRef[0].getBytes(StandardCharsets.UTF_8));
                             client.send(client.isResp(), Resp.array(resp), null);
                             served = true;
                             break;
                        }
                    }
                }
            }
            
            if (!served) {
                Carade.BlockingRequest bReq = new Carade.BlockingRequest(true, null, client.getDbIndex());
                for (String k : keys) {
                    Carade.blockingRegistry.computeIfAbsent(k, x -> new ConcurrentLinkedQueue<>()).add(bReq);
                }
                
                bReq.future.whenComplete((result, ex) -> {
                     if (ex != null) {
                         client.sendNull();
                     } else {
                         client.sendArray(result);
                     }
                });
                
                if (timeout > 0) {
                    client.scheduleTimeout(bReq, (long)(timeout * 1000));
                }
            }
        } catch (NumberFormatException e) {
            client.sendError("ERR timeout is not a float or out of range");
        }
    }
}
