package core.scripting;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RespParser {

    public static Object parse(byte[] input) {
        if (input == null || input.length == 0) return null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(input);
            return parseObject(in);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Object parseObject(ByteArrayInputStream in) throws IOException {
        int b = in.read();
        if (b == -1) return null;

        switch (b) {
            case '+': // Simple String
                return new SimpleString(readLine(in));
            case '-': // Error
                return new RespError(readLine(in));
            case ':': // Integer
                return Long.parseLong(readLine(in));
            case '$': // Bulk String
                return readBulkString(in);
            case '*': // Array
                return readArray(in);
            default:
                throw new IOException("Unknown RESP type: " + (char)b);
        }
    }

    private static String readLine(ByteArrayInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') break;
                sb.append((char)b);
                if (next != -1) sb.append((char)next);
            } else {
                sb.append((char)b);
            }
        }
        return sb.toString();
    }

    private static byte[] readBulkString(ByteArrayInputStream in) throws IOException {
        String lenStr = readLine(in);
        int len = Integer.parseInt(lenStr);
        if (len == -1) return null;

        byte[] bytes = new byte[len];
        int read = 0;
        while (read < len) {
            int r = in.read(bytes, read, len - read);
            if (r == -1) throw new IOException("Unexpected end of stream");
            read += r;
        }
        // Consume CRLF
        in.read();
        in.read();
        return bytes;
    }

    private static List<Object> readArray(ByteArrayInputStream in) throws IOException {
        String lenStr = readLine(in);
        int len = Integer.parseInt(lenStr);
        if (len == -1) return null; // Null array

        List<Object> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(parseObject(in));
        }
        return list;
    }
    
    public static class RespError {
        public final String message;
        public RespError(String message) { this.message = message; }
        @Override public String toString() { return message; }
    }

    public static class SimpleString {
        public final String value;
        public SimpleString(String value) { this.value = value; }
        @Override public String toString() { return value; }
    }
}
