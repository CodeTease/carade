package core;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class PubSub {
    // Channel -> Set of Subscribers
    private final ConcurrentHashMap<String, Set<Subscriber>> channels = new ConcurrentHashMap<>();
    // Pattern -> Set of Subscribers
    private final ConcurrentHashMap<String, Set<Subscriber>> patterns = new ConcurrentHashMap<>();
    
    // Subscriber -> Set of Channels (Reverse Index)
    private final ConcurrentHashMap<Subscriber, Set<String>> subToChannels = new ConcurrentHashMap<>();
    // Subscriber -> Set of Patterns (Reverse Index)
    private final ConcurrentHashMap<Subscriber, Set<String>> subToPatterns = new ConcurrentHashMap<>();

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
        subToChannels.computeIfAbsent(sub, k -> ConcurrentHashMap.newKeySet()).add(channel);
    }

    public void unsubscribe(String channel, Subscriber sub) {
        Set<Subscriber> subs = channels.get(channel);
        if (subs != null) {
            subs.remove(sub);
            if (subs.isEmpty()) channels.remove(channel);
        }
        
        Set<String> userChannels = subToChannels.get(sub);
        if (userChannels != null) {
            userChannels.remove(channel);
            if (userChannels.isEmpty()) subToChannels.remove(sub);
        }
    }
    
    public void unsubscribeAll(Subscriber sub) {
        // Fast Unsubscribe using Reverse Index
        Set<String> userChannels = subToChannels.remove(sub);
        if (userChannels != null) {
            for (String channel : userChannels) {
                Set<Subscriber> subs = channels.get(channel);
                if (subs != null) {
                    subs.remove(sub);
                    if (subs.isEmpty()) channels.remove(channel);
                }
            }
        }
        
        Set<String> userPatterns = subToPatterns.remove(sub);
        if (userPatterns != null) {
            for (String pattern : userPatterns) {
                Set<Subscriber> subs = patterns.get(pattern);
                if (subs != null) {
                    subs.remove(sub);
                    if (subs.isEmpty()) {
                        patterns.remove(pattern);
                        patternCache.remove(pattern);
                    }
                }
            }
        }
    }

    public void psubscribe(String pattern, Subscriber sub) {
        patterns.computeIfAbsent(pattern, k -> {
            // Compile and cache pattern when first subscriber appears
            patternCache.computeIfAbsent(pattern, this::compilePattern);
            return ConcurrentHashMap.newKeySet();
        }).add(sub);
        subToPatterns.computeIfAbsent(sub, k -> ConcurrentHashMap.newKeySet()).add(pattern);
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
        
        Set<String> userPatterns = subToPatterns.get(sub);
        if (userPatterns != null) {
            userPatterns.remove(pattern);
            if (userPatterns.isEmpty()) subToPatterns.remove(sub);
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

    public List<String> getChannels() {
        return new ArrayList<>(channels.keySet());
    }
    
    public int getNumSub(String channel) {
        Set<Subscriber> subs = channels.get(channel);
        return subs == null ? 0 : subs.size();
    }
}
