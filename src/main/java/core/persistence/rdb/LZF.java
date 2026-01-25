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
