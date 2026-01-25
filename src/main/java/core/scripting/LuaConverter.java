package core.scripting;

import org.luaj.vm2.*;
import java.util.List;
import java.util.ArrayList;

public class LuaConverter {

    public static LuaValue toLua(Object javaObject) {
        if (javaObject == null) {
            return LuaValue.FALSE; // Redis mapping: nil -> false in boolean context, but Lua nil is nil.
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
            return ((Boolean) javaObject) ? LuaValue.valueOf(1) : LuaValue.valueOf(0); 
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
