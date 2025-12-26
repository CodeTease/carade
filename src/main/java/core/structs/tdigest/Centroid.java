package core.structs.tdigest;

import java.io.Serializable;

public class Centroid implements Comparable<Centroid>, Serializable {
    private double mean;
    private long count;

    public Centroid(double mean, long count) {
        this.mean = mean;
        this.count = count;
    }

    public double getMean() {
        return mean;
    }

    public long getCount() {
        return count;
    }

    public void update(double mean, long count) {
        // Weighted average update
        this.mean = (this.mean * this.count + mean * count) / (this.count + count);
        this.count += count;
    }

    public void add(Centroid other) {
        update(other.mean, other.count);
    }

    @Override
    public int compareTo(Centroid o) {
        return Double.compare(this.mean, o.mean);
    }
    
    @Override
    public String toString() {
        return "Centroid{mean=" + mean + ", count=" + count + "}";
    }

    public Centroid copy() {
        return new Centroid(this.mean, this.count);
    }
}
