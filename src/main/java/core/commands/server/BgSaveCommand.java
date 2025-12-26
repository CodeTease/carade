package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class BgSaveCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (Carade.isSaving.get()) {
            client.sendError("ERR Background save already in progress");
            return;
        }
        
        new Thread(() -> {
            try {
                Carade.saveData();
            } catch (RuntimeException e) {
                // Ignore if it throws "in progress" because of race, but we checked before.
                // Or if called concurrently.
                System.err.println("Background save failed: " + e.getMessage());
            }
        }).start();
        
        client.sendSimpleString("Background saving started");
    }
}
