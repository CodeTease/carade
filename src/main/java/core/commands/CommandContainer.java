package core.commands;

public class CommandContainer {
    private final Command command;
    private final CommandMetadata metadata;

    public CommandContainer(Command command, CommandMetadata metadata) {
        this.command = command;
        this.metadata = metadata;
    }

    public Command getCommand() {
        return command;
    }

    public CommandMetadata getMetadata() {
        return metadata;
    }
}
