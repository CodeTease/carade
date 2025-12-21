package core.persistence.rdb;

public class LZF {
    public static void expand(byte[] in, int inLen, byte[] out, int outLen) {
        int i = 0; // input pointer
        int o = 0; // output pointer
        
        while (i < inLen) {
            int ctrl = in[i++] & 0xFF;

            if (ctrl < 32) {
                // Literal run
                int len = ctrl + 1;
                System.arraycopy(in, i, out, o, len);
                i += len;
                o += len;
            } else {
                // Back reference
                int len = ctrl >> 5;
                int ref = o - ((ctrl & 0x1F) << 8) - 1;

                if (len == 7) {
                    len += in[i++] & 0xFF;
                }

                ref -= in[i++] & 0xFF;
                len += 2;

                // Copy from reference
                // Note: ref and o might overlap, so we can't use System.arraycopy for overlapping regions easily if we are copying forward
                // But LZF copy is usually byte-by-byte or carefully handled.
                // In Java, System.arraycopy handles overlap correctly ONLY if src and dest are same array.
                // Here src is 'out' and dest is 'out'. So it should be fine?
                // Wait, System.arraycopy says: "If the src and dest arguments refer to the same array object, then the copying is performed as if the components at positions srcPos through srcPos+length-1 were first copied to a temporary array..."
                // This means it copies the *snapshot* of source.
                // But in LZF, we want to copy bytes that might have *just been written* if the overlap is small (run length > distance).
                // E.g. if distance is 1 and length is 10, we repeat the last byte 10 times.
                // System.arraycopy would copy the *old* values (zeros or whatever), not the newly written ones.
                // So for back references where overlap occurs, we must do it manually loop.
                
                if (o - ref < len) {
                    // Overlap case: distance < length
                    for (int k = 0; k < len; k++) {
                        out[o++] = out[ref++];
                    }
                } else {
                    System.arraycopy(out, ref, out, o, len);
                    o += len;
                }
            }
        }
    }
}
