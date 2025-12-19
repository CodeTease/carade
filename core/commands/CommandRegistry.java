package core.commands;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommandRegistry {
    private static final Map<String, Command> commands = new ConcurrentHashMap<>();

    public static void register(String name, Command command) {
        commands.put(name.toUpperCase(), command);
    }

    public static Command get(String name) {
        return commands.get(name.toUpperCase());
    }
}
