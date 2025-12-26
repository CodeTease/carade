package core.structs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.io.Serializable;
import java.util.NavigableSet;

public class CaradeZSet implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public final ConcurrentHashMap<String, Double> scores = new ConcurrentHashMap<>();
    public final ConcurrentSkipListSet<ZNode> sorted = new ConcurrentSkipListSet<>();

    public NavigableSet<ZNode> rangeByScore(double min, boolean minInclusive, double max, boolean maxInclusive) {
        ZNode start = null;
        if (min == Double.NEGATIVE_INFINITY && minInclusive) {
            start = null;
        } else {
            if (minInclusive) {
                start = new ZNode(min, "");
            } else {
                start = new ZNode(Math.nextUp(min), "");
            }
        }

        ZNode end = null;
        if (max == Double.POSITIVE_INFINITY && maxInclusive) {
            end = null;
        } else {
            if (maxInclusive) {
                if (max == Double.POSITIVE_INFINITY) end = null;
                else end = new ZNode(Math.nextUp(max), "");
            } else {
                end = new ZNode(max, "");
            }
        }

        if (start == null && end == null) return sorted;
        if (start == null) return sorted.headSet(end);
        if (end == null) return sorted.tailSet(start);
        return sorted.subSet(start, end);
    }

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

    public java.util.List<ZNode> popMin(int count) {
        java.util.List<ZNode> result = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            ZNode node = sorted.pollFirst();
            if (node == null) break;
            scores.remove(node.member);
            result.add(node);
        }
        return result;
    }

    public java.util.List<ZNode> popMax(int count) {
        java.util.List<ZNode> result = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            ZNode node = sorted.pollLast();
            if (node == null) break;
            scores.remove(node.member);
            result.add(node);
        }
        return result;
    }

    public CaradeZSet copy() {
        CaradeZSet copy = new CaradeZSet();
        copy.scores.putAll(this.scores);
        copy.sorted.addAll(this.sorted);
        return copy;
    }
}
