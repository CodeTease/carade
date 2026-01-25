package core;

import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MockClientHandler extends ClientHandler {
    public String lastError;
    public String lastResponse;
    public int dbIndex = 0;

    @Override
    public void sendError(String msg) {
        this.lastError = msg;
        this.lastResponse = "ERR " + msg; // Unified for easier checking
    }

    @Override
    public void sendResponse(byte[] respData, String raw) {
        if (raw != null) {
            this.lastResponse = raw;
        } else if (respData != null) {
            this.lastResponse = new String(respData, StandardCharsets.UTF_8);
        }
    }
    
    @Override
    public void sendSimpleString(String msg) {
        this.lastResponse = msg;
    }
    
    @Override
    public void sendBulkString(String s) {
        this.lastResponse = s;
    }
    
    @Override
    public void sendInteger(long i) {
        this.lastResponse = String.valueOf(i);
    }
    
    @Override
    public void sendArray(List<byte[]> list) {
        // For testing, capture the list as a String representation or just the first element
        // Simplest for unit tests checking values:
        if (list != null && !list.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for(int i=0; i<list.size(); i++) {
                sb.append(new String(list.get(i), StandardCharsets.UTF_8));
                if (i < list.size() - 1) sb.append(", ");
            }
            sb.append("]");
            this.lastResponse = sb.toString();
        } else {
            this.lastResponse = "[]";
        }
    }

    @Override
    public void executeWrite(Runnable r, String name, Object... args) {
        // Direct execution for tests
        r.run();
    }
    
    @Override
    public int getDbIndex() {
        return dbIndex;
    }
}
