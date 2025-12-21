package core.structs;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A simple HyperLogLog implementation.
 * 
 * Standard Redis HLL uses 16384 registers (2^14), with 6 bits per register.
 * We will use 8 bits (byte) per register for simplicity in Java.
 * M = 16384 (2^14).
 * Alpha_m for m=16384 is approx 0.7213 / (1 + 1.079/m).
 */
public class HyperLogLog implements Serializable {
    private static final int P = 14; 
    private static final int M = 1 << P; // 16384
    private static final double ALPHA_M = 0.7213 / (1 + 1.079/M);
    
    private final byte[] registers;
    private volatile boolean dirty = false;
    private volatile long cachedCount = -1;

    public HyperLogLog() {
        this.registers = new byte[M];
    }
    
    /**
     * Merge another HLL into this one.
     */
    public void merge(HyperLogLog other) {
        if (other == null) return;
        for (int i = 0; i < M; i++) {
            if (other.registers[i] > this.registers[i]) {
                this.registers[i] = other.registers[i];
                dirty = true;
            }
        }
    }

    /**
     * Add an element. Returns true if internal state changed.
     */
    public boolean add(String element) {
        long hash = MurmurHash64A.hash(element.getBytes());
        return add(hash);
    }
    
    public boolean add(long hash) {
        // First P bits for index
        int idx = (int) (hash >>> (64 - P));
        
        // Remaining 64 - P bits for run length of zeros
        // We look at the lower (64-P) bits. 
        // We want number of leading zeros in the remaining 50 bits + 1.
        // Long.numberOfLeadingZeros counts from high end.
        // So we mask out the index bits, shift up, count zeros.
        
        // Actually, standard HLL uses "number of leading zeros in the (w-p) bits".
        // Let's take the remaining bits:
        long w = hash << P; // Shift out the index bits
        // Now count leading zeros.
        // If w is 0, rank is 64-P + 1 (technically max possible).
        int rank = Long.numberOfLeadingZeros(w) - (P - 1); 
        // Wait, standard HLL usually:
        // x = hash(v)
        // j = x < 0..p-1 >
        // w = x < p..63 >
        // rho = 1 + lead_zeros(w)
        
        // In Java Long.numberOfLeadingZeros(x) returns 0..64.
        // If we shift left by P, we cleared top P bits.
        // But leading zeros will include those cleared bits? No, left shift pads with 0 at right, 
        // but high bits are discarded. 
        // Example: P=14. 
        // Hash = [14 bits index] [50 bits value]
        // hash << 14 moves [50 bits value] to top. Bottom 14 are 0.
        // numberOfLeadingZeros will count zeros at top of these 50 bits.
        // If all 50 bits are 0, nlz returns 64? No.
        // If hash << 14 is 0, then all 50 bits were 0. nlz = 64.
        // The max rank we care about is ~50.
        // If w=0, let's say rank is (64-P) + 1.
        
        int zeros = Long.numberOfLeadingZeros(hash << P);
        int rho = zeros + 1;
        
        if (rho > this.registers[idx]) {
            this.registers[idx] = (byte) rho;
            dirty = true;
            return true;
        }
        return false;
    }

    public long count() {
        if (!dirty && cachedCount >= 0) return cachedCount;
        
        double z = 0;
        for (int i = 0; i < M; i++) {
            z += Math.pow(2, -registers[i]);
        }
        
        double e = ALPHA_M * M * M / z;
        
        // Small range correction
        if (e <= 2.5 * M) {
            int v = 0;
            for (int i = 0; i < M; i++) {
                if (registers[i] == 0) v++;
            }
            if (v > 0) {
                e = M * Math.log((double)M / v);
            }
        } else if (e > (1L << 32) / 30.0) {
            // Large range correction (rarely needed for 64-bit hash usually)
            e = -Math.pow(2, 32) * Math.log(1 - e / Math.pow(2, 32));
        }
        
        long res = (long) e;
        cachedCount = res;
        dirty = false;
        return res;
    }

    // Inner MurmurHash64 implementation since we can't import easily
    private static class MurmurHash64A {
        public static long hash(byte[] data) {
            long m = 0xc6a4a7935bd1e995L;
            int r = 47;
            long h = 0x12345678 ^ (data.length * m);

            int length = data.length;
            int dataIndex = 0;

            while (length >= 8) {
                long k = getLong(data, dataIndex);
                dataIndex += 8;
                length -= 8;

                k *= m;
                k ^= k >>> r;
                k *= m;

                h ^= k;
                h *= m;
            }

            if (length > 0) {
                long k = 0;
                for(int i=0; i<length; i++) {
                     k |= ((long) data[dataIndex + i] & 0xFF) << (i * 8);
                }
                h ^= k;
                h *= m;
            }

            h ^= h >>> r;
            h *= m;
            h ^= h >>> r;
            return h;
        }

        private static long getLong(byte[] data, int index) {
            return ((long) data[index] & 0xff) |
                   (((long) data[index + 1] & 0xff) << 8) |
                   (((long) data[index + 2] & 0xff) << 16) |
                   (((long) data[index + 3] & 0xff) << 24) |
                   (((long) data[index + 4] & 0xff) << 32) |
                   (((long) data[index + 5] & 0xff) << 40) |
                   (((long) data[index + 6] & 0xff) << 48) |
                   (((long) data[index + 7] & 0xff) << 56);
        }
    }
}
