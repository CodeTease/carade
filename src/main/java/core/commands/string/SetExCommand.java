package core.commands.string;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class SetExCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 4) {
            client.sendError("usage: SETEX key seconds value");
            return;
        }
        // SETEX key seconds value -> SET key value EX seconds
        List<byte[]> newArgs = new ArrayList<>();
        newArgs.add("SET".getBytes(StandardCharsets.UTF_8));
        newArgs.add(args.get(1)); // key
        newArgs.add(args.get(3)); // value
        newArgs.add("EX".getBytes(StandardCharsets.UTF_8));
        newArgs.add(args.get(2)); // seconds
        
        new SetCommand().execute(client, newArgs);
    }
}
