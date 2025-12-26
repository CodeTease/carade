package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class ShutdownCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // Save?
        boolean save = true;
        if (args.size() > 1) {
            String arg = new String(args.get(1)).toUpperCase();
            if (arg.equals("NOSAVE")) save = false;
        }
        
        Carade.shutdownInitiated = true;

        if (save) {
            Carade.saveData();
        }
        
        // In a real server we might want to stop accepting new connections first.
        // Here we just exit.
        client.sendSimpleString("OK");
        client.close(); // Close current connection
        
        new Thread(() -> {
            try { Thread.sleep(100); } catch (InterruptedException e) {}
            System.exit(0);
        }).start();
    }
}
