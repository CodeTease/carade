import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class PubSub {
    // Channel -> Set of Subscribers
    private final ConcurrentHashMap<String, Set<Subscriber>> channels = new ConcurrentHashMap<>();
    // Pattern -> Set of Subscribers
    private final ConcurrentHashMap<String, Set<Subscriber>> patterns = new ConcurrentHashMap<>();
    
    // Pattern string -> Compiled Pattern
    private final ConcurrentHashMap<String, Pattern> patternCache = new ConcurrentHashMap<>();

    public interface Subscriber {
        void send(String channel, String message, String pattern);
        boolean isResp();
        // Identity check
        Object getId(); 
    }

    public void subscribe(String channel, Subscriber sub) {
        channels.computeIfAbsent(channel, k -> ConcurrentHashMap.newKeySet()).add(sub);
    }

    public void unsubscribe(String channel, Subscriber sub) {
        Set<Subscriber> subs = channels.get(channel);
        if (subs != null) {
            subs.remove(sub);
            if (subs.isEmpty()) channels.remove(channel);
        }
    }
    
    public void unsubscribeAll(Subscriber sub) {
        // This is slow, but acceptable for now
        for (Set<Subscriber> set : channels.values()) set.remove(sub);
        for (Map.Entry<String, Set<Subscriber>> entry : patterns.entrySet()) {
            Set<Subscriber> subs = entry.getValue();
            subs.remove(sub);
            if (subs.isEmpty()) {
                patterns.remove(entry.getKey());
                patternCache.remove(entry.getKey());
            }
        }
    }

    public void psubscribe(String pattern, Subscriber sub) {
        patterns.computeIfAbsent(pattern, k -> {
            // Compile and cache pattern when first subscriber appears
            patternCache.computeIfAbsent(pattern, this::compilePattern);
            return ConcurrentHashMap.newKeySet();
        }).add(sub);
    }

    public void punsubscribe(String pattern, Subscriber sub) {
        Set<Subscriber> subs = patterns.get(pattern);
        if (subs != null) {
            subs.remove(sub);
            if (subs.isEmpty()) {
                patterns.remove(pattern);
                patternCache.remove(pattern);
            }
        }
    }

    public int publish(String channel, String message) {
        int count = 0;
        
        // 1. Direct channels
        Set<Subscriber> direct = channels.get(channel);
        if (direct != null) {
            for (Subscriber sub : direct) {
                sub.send(channel, message, null);
                count++;
            }
        }

        // 2. Patterns
        for (Map.Entry<String, Set<Subscriber>> entry : patterns.entrySet()) {
            String pattern = entry.getKey();
            if (matches(pattern, channel)) {
                for (Subscriber sub : entry.getValue()) {
                    sub.send(channel, message, pattern);
                    count++;
                }
            }
        }
        return count;
    }
    
    // Glob-style matching: news.* matches news.sports
    private boolean matches(String pattern, String text) {
        Pattern p = patternCache.get(pattern);
        if (p == null) {
             // Fallback or lazy load if missing (shouldn't happen if logic is correct)
             p = compilePattern(pattern);
             patternCache.put(pattern, p);
        }
        return p.matcher(text).matches();
    }
    
    private Pattern compilePattern(String globPattern) {
        String regex = globPattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return Pattern.compile(regex);
    }
    
    public int getChannelCount() { return channels.size(); }
    public int getPatternCount() { return patterns.size(); }
}
