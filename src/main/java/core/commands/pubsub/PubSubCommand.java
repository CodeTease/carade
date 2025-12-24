package core.commands.pubsub;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PubSubCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: PUBSUB <subcommand> [argument [argument ...]]");
            return;
        }

        String sub = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();
        
        if (sub.equals("CHANNELS")) {
            // PUBSUB CHANNELS [pattern]
            // We need to access Carade.pubSub internals.
            // But Carade.pubSub only exposes getChannelCount().
            // We need to list channels.
            // Let's modify PubSub class or use reflection?
            // Better to modify PubSub class to expose `getChannels()`.
            // For now, let's assume we can modify PubSub.
            
            // Wait, I can't modify PubSub easily in this step without Plan update?
            // Actually I am in execution phase, I can modify any file.
            
            List<String> channels = Carade.pubSub.getChannels();
            String pattern = (args.size() > 2) ? new String(args.get(2), StandardCharsets.UTF_8) : null;
            
            List<byte[]> result = new ArrayList<>();
            for (String ch : channels) {
                if (pattern == null || matches(pattern, ch)) {
                    result.add(ch.getBytes(StandardCharsets.UTF_8));
                }
            }
            client.sendArray(result);
            
        } else if (sub.equals("NUMSUB")) {
            // PUBSUB NUMSUB [channel-1 ... channel-N]
            List<Object> result = new ArrayList<>();
            for (int i = 2; i < args.size(); i++) {
                String ch = new String(args.get(i), StandardCharsets.UTF_8);
                result.add(ch.getBytes(StandardCharsets.UTF_8));
                result.add((long) Carade.pubSub.getNumSub(ch));
            }
            client.sendMixedArray(result);
            
        } else if (sub.equals("NUMPAT")) {
            client.sendInteger(Carade.pubSub.getPatternCount());
        } else {
            client.sendError("ERR Unknown PUBSUB subcommand or wrong number of arguments for '" + sub + "'");
        }
    }
    
    // Glob matching helper
    private boolean matches(String pattern, String text) {
        // Simple glob to regex
         String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
         return java.util.regex.Pattern.compile(regex).matcher(text).matches();
    }
}
