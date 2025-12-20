package core.protocol;

import java.io.*;
import java.util.*;

public class Resp {
    public static final char ARRAY = '*';
    public static final char BULK_STRING = '$';
    public static final char SIMPLE_STRING = '+';
    public static final char ERROR = '-';
    public static final char INTEGER = ':';
    
    public static class Request {
        public List<byte[]> args;
        public boolean isResp;
        
        public Request(List<byte[]> args, boolean isResp) {
            this.args = args;
            this.isResp = isResp;
        }
    }
    
    // --- SERIALIZATION ---
    public static byte[] simpleString(String s) { 
        return ("+" + s + "\r\n").getBytes(java.nio.charset.StandardCharsets.UTF_8); 
    }
    
    public static byte[] error(String s) { 
        return ("-" + s + "\r\n").getBytes(java.nio.charset.StandardCharsets.UTF_8); 
    }
    
    public static byte[] integer(long i) { 
        return (":" + i + "\r\n").getBytes(java.nio.charset.StandardCharsets.UTF_8); 
    }
    
    // Legacy support for String args (converts to UTF-8 bytes)
    public static byte[] bulkString(String s) { 
        if (s == null) return "$-1\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return bulkString(b);
    }
    
    public static byte[] bulkString(byte[] b) {
        if (b == null) return "$-1\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            String header = "$" + (b != null ? b.length : 0) + "\r\n";
            bos.write(header.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            bos.write(b);
            bos.write("\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return bos.toByteArray();
        } catch (IOException e) { return null; }
    }

    public static byte[] mixedArray(List<Object> list) {
        if (list == null) return "*-1\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            String header = "*" + list.size() + "\r\n";
            bos.write(header.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            for (Object o : list) {
                if (o instanceof byte[]) {
                    bos.write(bulkString((byte[]) o));
                } else if (o instanceof Long || o instanceof Integer) {
                    bos.write(integer(((Number) o).longValue()));
                } else if (o instanceof List) {
                    bos.write(mixedArray((List<Object>) o));
                } else if (o == null) {
                    bos.write("$-1\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
            }
            return bos.toByteArray();
        } catch (IOException e) { return null; }
    }
    
    public static byte[] array(List<byte[]> list) {
        if (list == null) return "*-1\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            String header = "*" + list.size() + "\r\n";
            bos.write(header.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            for (byte[] b : list) {
                bos.write(bulkString(b));
            }
            return bos.toByteArray();
        } catch (IOException e) { return null; }
    }

    // --- PARSING ---
    public static Request parse(InputStream is) throws IOException {
        PushbackInputStream in = new PushbackInputStream(is);
        int firstByte = in.read();
        if (firstByte == -1) return null; // End of stream
        
        in.unread(firstByte);
        
        if (firstByte == ARRAY) {
            return new Request(parseRespArray(in), true);
        } else {
            return new Request(parseInline(in), false);
        }
    }

    private static List<byte[]> parseRespArray(InputStream in) throws IOException {
        int b = in.read();
        if (b != ARRAY) throw new IOException("Expected *");
        
        long count = readLong(in);
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(readBulkString(in));
        }
        return result;
    }

    private static byte[] readBulkString(InputStream in) throws IOException {
        int b = in.read();
        if (b != BULK_STRING) throw new IOException("Expected $");
        
        long len = readLong(in);
        if (len == -1) return null;
        
        byte[] bytes = new byte[(int)len];
        int read = 0;
        while (read < len) {
            int r = in.read(bytes, read, (int)len - read);
            if (r == -1) throw new IOException("Unexpected end of stream in BulkString");
            read += r;
        }
        // Consume \r\n
        if (in.read() != '\r' || in.read() != '\n') throw new IOException("Expected CRLF after BulkString");
        
        return bytes;
    }

    private static long readLong(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                if (in.read() != '\n') throw new IOException("Expected LF after CR");
                break;
            }
            sb.append((char)b);
        }
        return Long.parseLong(sb.toString());
    }

    private static List<byte[]> parseInline(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break; 
            buffer.write(b);
        }
        String line = buffer.toString().trim(); // This uses default charset, acceptable for inline
        if (line.isEmpty()) return Collections.emptyList();
        
        List<byte[]> list = new ArrayList<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
        while (m.find()) {
            String match = m.group(1);
            if (match.startsWith("\"") && match.endsWith("\"")) {
                list.add(match.substring(1, match.length() - 1).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } else {
                list.add(match.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        return list;
    }
}
