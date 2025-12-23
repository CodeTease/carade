package core.structs.tdigest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TDigest implements Serializable {
    private List<Centroid> centroids;
    private List<Centroid> buffer;
    private double compression;
    private long totalCount;
    private static final int BUFFER_MULTIPLIER = 10;

    public TDigest() {
        this(100.0);
    }

    public TDigest(double compression) {
        this.compression = compression;
        this.centroids = new ArrayList<>();
        this.buffer = new ArrayList<>();
        this.totalCount = 0;
    }

    public synchronized void add(double value) {
        add(value, 1);
    }

    public synchronized void add(double value, long count) {
        buffer.add(new Centroid(value, count));
        totalCount += count;
        // Merge if buffer is too large
        if (buffer.size() > compression * BUFFER_MULTIPLIER) {
            merge();
        }
    }

    private void merge() {
        if (buffer.isEmpty() && centroids.isEmpty()) return;

        List<Centroid> all = new ArrayList<>(centroids.size() + buffer.size());
        all.addAll(centroids);
        all.addAll(buffer);
        Collections.sort(all);

        centroids.clear();
        buffer.clear();

        if (all.isEmpty()) return;

        Centroid current = all.get(0);
        double weightSoFar = 0;

        for (int i = 1; i < all.size(); i++) {
            Centroid next = all.get(i);
            
            double proposedWeight = current.getCount() + next.getCount();
            
            boolean canMerge = false;
            if (totalCount > 0) {
                 double q = (weightSoFar + current.getCount() / 2.0) / totalCount;
                 double kLimit = 4 * totalCount * q * (1 - q) / compression;
                 
                 if (proposedWeight <= Math.max(1, kLimit)) {
                     canMerge = true;
                 }
            } else {
                canMerge = true; 
            }
            
            if (!canMerge) {
                double qStart = weightSoFar / totalCount;
                double qEnd = (weightSoFar + proposedWeight) / totalCount;
                
                double kStart = compression * (Math.asin(2 * qStart - 1) / Math.PI + 0.5);
                double kEnd = compression * (Math.asin(2 * qEnd - 1) / Math.PI + 0.5);
                
                if (kEnd - kStart <= 1.0) {
                    canMerge = true;
                }
            }

            if (canMerge) {
                current.add(next);
            } else {
                centroids.add(current);
                weightSoFar += current.getCount();
                current = next;
            }
        }
        centroids.add(current);
    }
    
    public synchronized void compress() {
        merge();
    }

    public synchronized double quantile(double q) {
        compress(); 
        
        if (centroids.isEmpty()) return Double.NaN;
        if (centroids.size() == 1) return centroids.get(0).getMean();
        
        double rank = q * totalCount;
        
        // Special handling for first and last
        if (rank < centroids.get(0).getCount() / 2.0) {
            return centroids.get(0).getMean(); 
        }
        
        if (rank > totalCount - centroids.get(centroids.size()-1).getCount() / 2.0) {
             return centroids.get(centroids.size()-1).getMean();
        }

        double weightSoFar = centroids.get(0).getCount() / 2.0;
        
        for (int i = 0; i < centroids.size() - 1; i++) {
            Centroid c1 = centroids.get(i);
            Centroid c2 = centroids.get(i+1);
            
            double step = (c1.getCount() / 2.0) + (c2.getCount() / 2.0);
            
            if (weightSoFar + step > rank) {
                double p = (rank - weightSoFar) / step;
                return c1.getMean() + p * (c2.getMean() - c1.getMean());
            }
            
            weightSoFar += step;
        }
        
        return centroids.get(centroids.size()-1).getMean();
    }
    
    public synchronized long size() {
        return totalCount;
    }
    
    public synchronized int centroidCount() {
        return centroids.size() + buffer.size();
    }
    
    public double getCompression() {
        return compression;
    }
}
