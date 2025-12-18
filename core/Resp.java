import java.io.*;
import java.util.*;

public class Resp {
    public static final char ARRAY = '*';
    public static final char BULK_STRING = '$';
    public static final char SIMPLE_STRING = '+';
    public static final char ERROR = '-';
    public static final char INTEGER = ':';
    
    public static class Request {
        public List<String> args;
        public boolean isResp;
        
        public Request(List<String> args, boolean isResp) {
            this.args = args;
            this.isResp = isResp;
        }
    }
    
    // --- SERIALIZATION ---
    public static String simpleString(String s) { return "+" + s + "\r\n"; }
    public static String error(String s) { return "-" + s + "\r\n"; }
    public static String integer(long i) { return ":" + i + "\r\n"; }
    public static String bulkString(String s) { 
        if (s == null) return "$-1\r\n";
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return "$" + b.length + "\r\n" + s + "\r\n";
    }
    
    public static byte[] bulkStringBytes(byte[] b) {
        if (b == null) return "$-1\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            String header = "$" + b.length + "\r\n";
            bos.write(header.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            bos.write(b);
            bos.write("\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return bos.toByteArray();
        } catch (IOException e) { return null; }
    }
    
    // Helper to allow writing bytes directly would be better, but requires changing callers.
    // For this specific task, we will try to handle it.
    public static String array(List<String> list) {
        if (list == null) return "*-1\r\n";
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(list.size()).append("\r\n");
        for (String s : list) {
            sb.append(bulkString(s));
        }
        return sb.toString();
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

    private static List<String> parseRespArray(InputStream in) throws IOException {
        int b = in.read();
        if (b != ARRAY) throw new IOException("Expected *");
        
        long count = readLong(in);
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(readBulkString(in));
        }
        return result;
    }

    private static String readBulkString(InputStream in) throws IOException {
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
        
        return new String(bytes); // Assume UTF-8
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

    private static List<String> parseInline(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break; 
            buffer.write(b);
        }
        String line = buffer.toString().trim();
        if (line.isEmpty()) return Collections.emptyList();
        
        List<String> list = new ArrayList<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
        while (m.find()) {
            String match = m.group(1);
            if (match.startsWith("\"") && match.endsWith("\"")) list.add(match.substring(1, match.length() - 1));
            else list.add(match);
        }
        return list;
    }
}
