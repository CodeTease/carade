package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BgRewriteAofCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        CompletableFuture.runAsync(() -> {
            System.out.println("🔄 Starting Background AOF Rewrite...");
            Carade.aofHandler.rewrite(Carade.db);
        });
        client.sendSimpleString("Background append only file rewriting started");
    }
}
