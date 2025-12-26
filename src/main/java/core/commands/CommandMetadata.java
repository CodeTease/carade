package core.commands;

import java.util.Collections;
import java.util.Set;

public class CommandMetadata {
    private final int arity;
    private final Set<String> flags;
    private final int firstKey;
    private final int lastKey;
    private final int step;

    public CommandMetadata(int arity, Set<String> flags, int firstKey, int lastKey, int step) {
        this.arity = arity;
        this.flags = flags != null ? flags : Collections.emptySet();
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.step = step;
    }

    public int getArity() {
        return arity;
    }

    public Set<String> getFlags() {
        return flags;
    }

    public int getFirstKey() {
        return firstKey;
    }

    public int getLastKey() {
        return lastKey;
    }

    public int getStep() {
        return step;
    }
}
