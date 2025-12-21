package core.commands.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;

public class JsonUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static JsonNode parse(String json) throws IOException {
        return mapper.readTree(json);
    }
    
    public static String stringify(JsonNode node) {
        return node.toString();
    }
    
    /**
     * Traverses the JsonNode based on the path (e.g., "$", "$.a.b").
     * Returns null if path not found.
     */
    public static JsonNode getByPath(JsonNode root, String path) {
        if (root == null) return null;
        if (path == null || path.equals("$") || path.equals(".")) {
            return root;
        }
        
        // Remove leading $. if present
        if (path.startsWith("$.")) {
            path = path.substring(2);
        } else if (path.startsWith(".")) {
            path = path.substring(1);
        }
        
        String[] parts = path.split("\\.");
        JsonNode current = root;
        
        for (String part : parts) {
            if (current == null) return null;
            if (current.isObject()) {
                current = current.get(part);
            } else if (current.isArray()) {
                try {
                    int index = Integer.parseInt(part);
                    current = current.get(index);
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return current;
    }
    
    /**
     * Updates the JsonNode at the given path.
     * Returns the modified root node.
     */
    public static JsonNode setByPath(JsonNode root, String path, JsonNode newValue) {
        if (path == null || path.equals("$") || path.equals(".")) {
            return newValue;
        }
        
        // Remove leading $. if present
        if (path.startsWith("$.")) {
            path = path.substring(2);
        } else if (path.startsWith(".")) {
            path = path.substring(1);
        }
        
        String[] parts = path.split("\\.");
        return setRecursive(root, parts, 0, newValue);
    }
    
    private static JsonNode setRecursive(JsonNode current, String[] parts, int index, JsonNode newValue) {
        String part = parts[index];
        boolean isLast = index == parts.length - 1;
        
        if (isLast) {
            if (current.isObject()) {
                ((ObjectNode) current).set(part, newValue);
                return current;
            } else if (current.isArray()) {
                 try {
                    int idx = Integer.parseInt(part);
                    // Resize array if needed? For now strict update.
                    // Actually, ArrayNode.set(index, value) throws if index is out of bounds for some versions,
                    // or just expands. Jackson ArrayNode.set replaces or adds if index >= size?
                    // Let's check Jackson behavior. usually set(int, JsonNode) inserts nulls if needed.
                    // But safe way:
                    ArrayNode arr = (ArrayNode) current;
                    while (arr.size() <= idx) {
                        arr.addNull();
                    }
                    arr.set(idx, newValue);
                    return current;
                } catch (NumberFormatException e) {
                    return current; // fail silently
                }
            }
            return current;
        }
        
        // Navigation
        JsonNode next = null;
        if (current.isObject()) {
            next = current.get(part);
            if (next == null) {
                // Determine if next should be object or array?
                // Heuristic: check if next part is integer.
                if (index + 1 < parts.length) {
                    String nextPart = parts[index+1];
                    if (isInteger(nextPart)) {
                        next = mapper.createArrayNode();
                    } else {
                        next = mapper.createObjectNode();
                    }
                    ((ObjectNode) current).set(part, next);
                }
            }
        } else if (current.isArray()) {
             try {
                int idx = Integer.parseInt(part);
                ArrayNode arr = (ArrayNode) current;
                while (arr.size() <= idx) {
                    arr.addNull();
                }
                next = arr.get(idx);
                if (next == null || next.isNull()) {
                     if (index + 1 < parts.length) {
                        String nextPart = parts[index+1];
                        if (isInteger(nextPart)) {
                            next = mapper.createArrayNode();
                        } else {
                            next = mapper.createObjectNode();
                        }
                        arr.set(idx, next);
                    }
                }
            } catch (NumberFormatException e) {
                return current;
            }
        }
        
        if (next != null) {
            setRecursive(next, parts, index + 1, newValue);
        }
        
        return current;
    }
    
    private static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch(NumberFormatException e) {
            return false;
        }
    }
}
