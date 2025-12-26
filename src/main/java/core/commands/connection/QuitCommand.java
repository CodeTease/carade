package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class QuitCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // The implementation in ClientHandler just closes the context.
        // We can do that here, but ClientHandler logic might want to do it cleanly.
        // Actually, ClientHandler checks for "QUIT" explicitly in some places, 
        // but if we register it, this will be called.
        // ClientHandler's send method sends to ctx.
        // We need to close the channel.
        // We can access ctx via ClientHandler? No, ctx is private.
        // But send "OK" then close?
        client.sendSimpleString("OK");
        // We don't have direct access to close() on ClientHandler.
        // But ClientHandler.channelRead handles "QUIT" specially in the "subs" mode.
        // For normal mode, it was in the switch.
        // ClientHandler has no public close() method?
        // Wait, ClientHandler.channelInactive calls cleanup.
        // We can throw an exception or we need a way to close connection.
        // Looking at ClientHandler.java: "ctx.close()" was called.
        // I should probably add a close() method to ClientHandler or similar.
        // For now, I will assume I can modify ClientHandler later to support this, 
        // or I can leave QUIT in ClientHandler if it's special.
        // BUT, the goal is to remove the switch.
        // I'll add a public close() method to ClientHandler when I modify it.
    }
}
