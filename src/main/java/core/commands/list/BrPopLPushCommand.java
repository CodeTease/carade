package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.List;

public class BrPopLPushCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
             client.sendError("usage: BRPOPLPUSH source destination timeout");
             return;
        }

        try {
            double timeout = Double.parseDouble(new String(args.get(3), StandardCharsets.UTF_8));
            String source = new String(args.get(1), StandardCharsets.UTF_8);
            String destKey = new String(args.get(2), StandardCharsets.UTF_8);
            
            boolean served = false;
            ValueEntry entry = Carade.db.get(client.getDbIndex(), source);
            if (entry != null && entry.type == DataType.LIST) {
                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.getValue();
                if (!list.isEmpty()) {
                    final String[] valRef = {null};
                    
                    client.executeWrite(() -> {
                        ValueEntry e = Carade.db.get(client.getDbIndex(), source);
                        if (e != null && e.type == DataType.LIST) {
                            ConcurrentLinkedDeque<String> l = (ConcurrentLinkedDeque<String>) e.getValue();
                            valRef[0] = l.pollLast(); // RPOPLPUSH pops from tail
                            
                            if (valRef[0] != null) {
                                if (l.isEmpty()) Carade.db.remove(client.getDbIndex(), source);
                                Carade.notifyWatchers(source);
                                
                                client.executeWrite(() -> {
                                    Carade.db.getStore(client.getDbIndex()).compute(destKey, (dk, dv) -> {
                                         if (dv == null) {
                                             ConcurrentLinkedDeque<String> dl = new ConcurrentLinkedDeque<>();
                                             dl.addFirst(valRef[0]);
                                             return new ValueEntry(dl, DataType.LIST, -1);
                                         } else if (dv.type == DataType.LIST) {
                                             ConcurrentLinkedDeque<String> dl = (ConcurrentLinkedDeque<String>) dv.getValue();
                                             dl.addFirst(valRef[0]);
                                             return dv;
                                         }
                                         return dv; // Should handle error?
                                     });
                                     Carade.notifyWatchers(destKey);
                                }, "LPUSH", destKey, valRef[0]);
                            }
                        }
                    }, "RPOPLPUSH", source, destKey);
                    
                    if (valRef[0] != null) {
                         client.sendBulkString(valRef[0]);
                         served = true;
                    }
                }
            }
            
            if (!served) {
                // Async blocking
                // Note: BlockingRequest in Carade.java handles targetKey (push logic)
                Carade.BlockingRequest bReq = new Carade.BlockingRequest(false, destKey, client.getDbIndex());
                Carade.blockingRegistry.computeIfAbsent(source, x -> new ConcurrentLinkedQueue<>()).add(bReq);
                
                bReq.future.whenComplete((result, ex) -> {
                     if (ex != null) {
                         client.sendNull();
                     } else {
                         // result is [source, value]
                         if (result.size() >= 2) {
                              client.sendBulkString(new String(result.get(1), StandardCharsets.UTF_8));
                         } else {
                              client.sendNull();
                         }
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
