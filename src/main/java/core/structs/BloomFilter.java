package core.structs;

import java.io.Serializable;
import java.util.BitSet;
import java.nio.charset.StandardCharsets;

public class BloomFilter implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final BitSet bitSet;
    private final int bitSize;
    private final int hashCount;
    
    // Default to ~10k items with 0.01 error rate
    // m = -n*ln(p) / (ln(2)^2)
    // k = m/n * ln(2)
    // For n=10000, p=0.01:
    // m = 95850 bits (~12KB)
    // k = 7
    public BloomFilter(int n, double p) {
        if (n <= 0) n = 10000;
        if (p <= 0) p = 0.01;
        
        int m = (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
        int k = (int) (m / (double)n * Math.log(2));
        
        this.bitSize = m;
        this.hashCount = k;
        this.bitSet = new BitSet(bitSize);
    }
    
    public BloomFilter() {
        this(10000, 0.01);
    }

    public int add(String item) {
        byte[] bytes = item.getBytes(StandardCharsets.UTF_8);
        boolean changed = false;
        int h1 = murmur3(bytes, 0);
        int h2 = murmur3(bytes, h1);
        
        for (int i = 0; i < hashCount; i++) {
            int combined = Math.abs((h1 + i * h2) % bitSize);
            if (!bitSet.get(combined)) {
                bitSet.set(combined);
                changed = true;
            }
        }
        return changed ? 1 : 0;
    }

    public int exists(String item) {
        byte[] bytes = item.getBytes(StandardCharsets.UTF_8);
        int h1 = murmur3(bytes, 0);
        int h2 = murmur3(bytes, h1);
        
        for (int i = 0; i < hashCount; i++) {
            int combined = Math.abs((h1 + i * h2) % bitSize);
            if (!bitSet.get(combined)) return 0;
        }
        return 1;
    }

    // Simplified Murmur3 32-bit (adaptation)
    private int murmur3(byte[] data, int seed) {
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        int h1 = seed;
        int roundedEnd = (data.length & 0xfffffffc);  // round down to 4 byte block

        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        switch (data.length & 3) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        h1 ^= data.length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }
}
