package core.commands;

import core.network.ClientHandler;
import java.util.List;

public interface Command {
    // Executes the command logic.
    // The command is responsible for sending the response using client.sendResponse() (or equivalent)
    void execute(ClientHandler client, List<byte[]> args);
}
