package core.scripting;

import org.luaj.vm2.*;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

public class LuaConverter {

    public static LuaValue toLua(Object javaObject) {
        if (javaObject == null) {
            return LuaValue.FALSE; // Redis mapping: nil -> false in boolean context, but Lua nil is nil.
            // Wait, "Redis Lua coi nil là false trong boolean context."
            // But if we return Java null, usually we want Lua nil.
            // LuaValue.NIL is what we want.
            // But Redis docs say: "Redis Nil -> Lua boolean false" (legacy behavior)
            // Actually:
            // "Redis Nil bulk reply and Nil multi bulk reply -> Lua false boolean type"
            // So if RESP is $-1, we return false.
            // If RESP is *-1, we return false.
            // But if it's Java null generally? Let's use false to match Redis behavior.
        }
        
        if (javaObject instanceof Long || javaObject instanceof Integer) {
            return LuaValue.valueOf(((Number) javaObject).longValue());
        }
        
        if (javaObject instanceof Double || javaObject instanceof Float) {
             return LuaValue.valueOf(((Number) javaObject).doubleValue());
        }
        
        if (javaObject instanceof String) {
            return LuaString.valueOf((String) javaObject);
        }

        if (javaObject instanceof RespParser.SimpleString) {
             LuaTable table = new LuaTable();
             table.set("ok", LuaString.valueOf(((RespParser.SimpleString) javaObject).value));
             return table;
        }
        
        if (javaObject instanceof byte[]) {
            return LuaString.valueOf((byte[]) javaObject);
        }
        
        if (javaObject instanceof Boolean) {
            return ((Boolean) javaObject) ? LuaValue.valueOf(1) : LuaValue.valueOf(0); // Redis specific? Or Lua boolean?
            // "Redis Boolean -> Lua boolean false/true?"
            // No, Redis doesn't have native boolean.
            // But internal command response might be boolean? No, usually integer 0/1.
            // If Java object is boolean, we should probably return Lua boolean.
            // But wait, the design says:
            // "Boolean -> LuaNumber (0 hoặc 1)" in table "Java / Carade DB -> Lua Type".
            // So if we have boolean, return 0 or 1.
        }

        if (javaObject instanceof List) {
            List<?> list = (List<?>) javaObject;
            LuaTable table = new LuaTable();
            for (int i = 0; i < list.size(); i++) {
                table.set(i + 1, toLua(list.get(i))); // Lua uses 1-based indexing
            }
            return table;
        }

        if (javaObject instanceof RespParser.RespError) {
             // For pcall to catch it, or return table {err="msg"}?
             // redis.pcall returns {err="msg"} on error.
             // redis.call raises error.
             // If we are converting a result from a command, and it's an error...
             // It depends on who is calling. 
             // But purely data conversion:
             // Redis Error reply -> Lua table with a single field err
             LuaTable table = new LuaTable();
             table.set("err", LuaString.valueOf(((RespParser.RespError) javaObject).message));
             return table;
        }
        
        return LuaString.valueOf(javaObject.toString());
    }

    public static Object toJava(LuaValue luaValue) {
        if (luaValue.isnil()) {
            return null;
        }
        
        if (luaValue.isboolean()) {
             // Lua boolean -> Java Boolean? Or 0/1?
             // If we are returning to Redis (RESP), we usually return 0/1 or null.
             // But `redis.call` is internal. 
             // When returning from EVAL to client:
             // Lua boolean true -> Redis integer reply with value of 1.
             // Lua boolean false -> Redis Nil bulk reply.
             // Wait, let's check Redis docs.
             // "Lua boolean true -> Redis integer reply 1."
             // "Lua boolean false -> Redis Nil bulk reply." (Wait, is it nil or 0?)
             // Docs: "Lua boolean false -> Redis Nil bulk reply." (i.e. null)
             return luaValue.toboolean() ? 1L : null;
        }
        
        if (luaValue.isint() || luaValue.islong()) {
            return luaValue.tolong();
        }
        
        if (luaValue.isnumber()) {
            // Check if integer
            double d = luaValue.todouble();
            if (d == (long) d) return (long) d;
            return (long) d;
        }
        
        if (luaValue.isstring()) {
            // Lua string -> Redis bulk reply
            LuaString s = luaValue.checkstring();
            byte[] bytes = new byte[s.m_length];
            s.copyInto(0, bytes, 0, s.m_length);
            return bytes;
        }
        
        if (luaValue.istable()) {
            // Lua table (array) -> Redis multi bulk reply
            // Lua table (single field 'err') -> Redis error reply
            // Lua table (single field 'ok') -> Redis status reply
            LuaTable table = (LuaTable) luaValue;
            
            LuaValue err = table.get("err");
            if (!err.isnil()) {
                // It's an error
                return new RespParser.RespError(err.tojstring());
            }
            
            LuaValue ok = table.get("ok");
            if (!ok.isnil()) {
                // It's a simple string status
                return ok.tojstring(); // Will be treated as Simple String by caller? 
                // We need to differentiate Simple String from Bulk String (byte[]).
                // Let's return String for Simple String, byte[] for Bulk.
            }
            
            // Array
            // Iterate 1..N
            // Lua arrays are 1-based.
            // We need to find the max index. table.length() or table.keyCount()?
            // table.length() (the # operator)
            int len = table.length();
            List<Object> list = new ArrayList<>(len);
            for (int i = 1; i <= len; i++) {
                list.add(toJava(table.get(i)));
            }
            return list;
        }
        
        return luaValue.toString();
    }
}
