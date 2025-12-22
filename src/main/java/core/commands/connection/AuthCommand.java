package core.commands.connection;

import core.Carade;
import core.Config;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class AuthCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: AUTH [user] password");
            return;
        }
        
        String user = "default";
        String pass = "";
        
        if (args.size() == 2) {
            pass = new String(args.get(1), StandardCharsets.UTF_8);
        } else {
            user = new String(args.get(1), StandardCharsets.UTF_8);
            pass = new String(args.get(2), StandardCharsets.UTF_8);
        }
        
        Config.User u = Carade.config.users.get(user);
        // Note: ClientHandler.currentUser is private. I need to add a setter.
        // I will assume I will add `setCurrentUser` to ClientHandler.
        if (u != null && u.password.equals(pass)) {
            client.setCurrentUser(u);
            client.sendSimpleString("OK");
        } else {
            client.sendError("WRONGPASS invalid username-password pair");
        }
    }
}
