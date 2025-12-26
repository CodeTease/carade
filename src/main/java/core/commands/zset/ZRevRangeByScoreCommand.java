package core.commands.zset;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class ZRevRangeByScoreCommand implements Command {
    private final ZRangeByScoreCommand delegate = new ZRangeByScoreCommand();
    
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // Reuse ZRangeByScoreCommand logic which handles ZREVRANGEBYSCORE via command name check
        // We just need to make sure args[0] is ZREVRANGEBYSCORE if not passed properly?
        // Actually, the command name is passed in args[0].
        // If we register this class for ZREVRANGEBYSCORE, args[0] will be ZREVRANGEBYSCORE.
        // ZRangeByScoreCommand logic checks `cmd.equals("ZRANGEBYSCORE")` or else implies reverse.
        // So we can just delegate.
        delegate.execute(client, args);
    }
}
