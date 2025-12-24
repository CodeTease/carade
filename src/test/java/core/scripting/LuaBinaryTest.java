package core.scripting;

import org.junit.jupiter.api.Test;
import org.luaj.vm2.*;
import java.util.List;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class LuaBinaryTest {

    @Test
    public void testBinarySafety() {
        // Create a byte array with invalid UTF-8 sequence
        byte[] binaryData = new byte[] { (byte)0xFF, (byte)0xFE, 0x00, 0x01 };
        
        // Java -> Lua
        LuaValue luaVal = LuaConverter.toLua(binaryData);
        assertTrue(luaVal.isstring());
        
        // Check length
        assertEquals(4, luaVal.length());
        
        // Lua -> Java
        Object javaVal = LuaConverter.toJava(luaVal);
        assertTrue(javaVal instanceof byte[]);
        assertArrayEquals(binaryData, (byte[]) javaVal);
    }
}
