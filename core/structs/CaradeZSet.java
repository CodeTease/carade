package core.structs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.io.Serializable;

public class CaradeZSet implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public final ConcurrentHashMap<String, Double> scores = new ConcurrentHashMap<>();
    public final ConcurrentSkipListSet<ZNode> sorted = new ConcurrentSkipListSet<>();

    public int add(double score, String member) {
        Double oldScore = scores.get(member);
        if (oldScore != null) {
            if (oldScore == score) return 0;
            sorted.remove(new ZNode(oldScore, member));
            scores.put(member, score);
            sorted.add(new ZNode(score, member));
            return 0; // Updated
        }
        scores.put(member, score);
        sorted.add(new ZNode(score, member));
        return 1; // New
    }

    public Double score(String member) {
        return scores.get(member);
    }

    public int size() {
        return scores.size();
    }

    public double incrBy(double increment, String member) {
        Double oldScore = scores.get(member);
        double newScore = (oldScore == null ? 0 : oldScore) + increment;
        add(newScore, member);
        return newScore;
    }
}
