package core.server;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.List;
import java.util.ArrayList;

public class SlowlogManager {
    public static final ConcurrentLinkedDeque<String> slowLog = new ConcurrentLinkedDeque<>();
    public static final int SLOWLOG_MAX_LEN = 128;

    public static void log(String entry) {
        slowLog.add(entry);
        while (slowLog.size() > SLOWLOG_MAX_LEN) {
            slowLog.poll();
        }
    }

    public static void clear() {
        slowLog.clear();
    }

    public static int size() {
        return slowLog.size();
    }

    public static List<String> getLogs() {
        return new ArrayList<>(slowLog);
    }
}
