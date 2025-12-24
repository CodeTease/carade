package core.scripting;

import org.junit.jupiter.api.Test;
import org.luaj.vm2.*;
import java.util.List;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class LuaConverterTest {

    @Test
    public void testJavaToLua() {
        // Integer
        assertEquals(LuaValue.valueOf(123), LuaConverter.toLua(123));
        assertEquals(LuaValue.valueOf(123), LuaConverter.toLua(123L));

        // String
        assertEquals(LuaString.valueOf("hello"), LuaConverter.toLua("hello"));
        assertEquals(LuaString.valueOf("hello"), LuaConverter.toLua("hello".getBytes(StandardCharsets.UTF_8)));

        // Boolean
        assertEquals(LuaValue.valueOf(1), LuaConverter.toLua(true));
        assertEquals(LuaValue.valueOf(0), LuaConverter.toLua(false));

        // List
        List<String> list = List.of("a", "b");
        LuaValue luaList = LuaConverter.toLua(list);
        assertTrue(luaList.istable());
        assertEquals(2, luaList.length());
        assertEquals("a", luaList.get(1).tojstring());
        assertEquals("b", luaList.get(2).tojstring());
        
        // Null
        assertEquals(LuaValue.FALSE, LuaConverter.toLua(null));
        
        // Error
        RespParser.RespError err = new RespParser.RespError("Some error");
        LuaValue luaErr = LuaConverter.toLua(err);
        assertTrue(luaErr.istable());
        assertEquals("Some error", luaErr.get("err").tojstring());
        
        // Simple String
        RespParser.SimpleString ss = new RespParser.SimpleString("OK");
        LuaValue luaSs = LuaConverter.toLua(ss);
        assertTrue(luaSs.istable());
        assertEquals("OK", luaSs.get("ok").tojstring());
    }

    @Test
    public void testLuaToJava() {
        // Integer
        assertEquals(123L, LuaConverter.toJava(LuaValue.valueOf(123)));

        // String
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), (byte[]) LuaConverter.toJava(LuaString.valueOf("hello")));

        // Boolean
        assertEquals(1L, LuaConverter.toJava(LuaValue.TRUE));
        assertNull(LuaConverter.toJava(LuaValue.FALSE)); // False -> Nil (null)

        // Table Array
        LuaTable table = new LuaTable();
        table.set(1, LuaString.valueOf("a"));
        table.set(2, LuaString.valueOf("b"));
        Object javaList = LuaConverter.toJava(table);
        assertTrue(javaList instanceof List);
        List<?> list = (List<?>) javaList;
        assertEquals(2, list.size());
        assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), (byte[]) list.get(0));

        // Table Error
        LuaTable errTable = new LuaTable();
        errTable.set("err", LuaString.valueOf("Some error"));
        Object javaErr = LuaConverter.toJava(errTable);
        assertTrue(javaErr instanceof RespParser.RespError);
        assertEquals("Some error", ((RespParser.RespError) javaErr).message);
        
        // Table Ok
        LuaTable okTable = new LuaTable();
        okTable.set("ok", LuaString.valueOf("OK"));
        Object javaOk = LuaConverter.toJava(okTable);
        assertTrue(javaOk instanceof String);
        assertEquals("OK", javaOk);
    }
}
