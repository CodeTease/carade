package core.utils;

import java.util.concurrent.atomic.AtomicReference;

public class Time {
    public interface Clock {
        long currentTimeMillis();
    }

    private static final Clock SYSTEM_CLOCK = System::currentTimeMillis;
    private static final AtomicReference<Clock> clock = new AtomicReference<>(SYSTEM_CLOCK);

    public static long now() {
        return clock.get().currentTimeMillis();
    }

    public static void setClock(Clock newClock) {
        clock.set(newClock);
    }
    
    public static void useSystemClock() {
        clock.set(SYSTEM_CLOCK);
    }
}
