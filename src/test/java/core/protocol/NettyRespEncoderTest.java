package core.protocol;

import core.protocol.netty.NettyRespEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class NettyRespEncoderTest {

    @Test
    public void testEncodeSimpleTypes() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespEncoder());
        
        // Test byte[] pass-through (Simulating what ClientHandler does)
        byte[] resp = Resp.simpleString("OK");
        assertTrue(channel.writeOutbound(resp));
        
        ByteBuf out = channel.readOutbound();
        assertEquals("+OK\r\n", out.toString(StandardCharsets.UTF_8));
        out.release();
    }
    
    @Test
    public void testEncodeNestedArrays() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespEncoder());
        
        // Create mixed array: ["foo", 123, ["nested"]]
        List<Object> nested = new ArrayList<>();
        nested.add("nested".getBytes(StandardCharsets.UTF_8));
        
        List<Object> list = new ArrayList<>();
        list.add("foo".getBytes(StandardCharsets.UTF_8));
        list.add(123L);
        list.add(nested);
        
        byte[] encoded = Resp.mixedArray(list);
        
        assertTrue(channel.writeOutbound(encoded));
        ByteBuf out = channel.readOutbound();
        String output = out.toString(StandardCharsets.UTF_8);
        
        // Expected: *3\r\n$3\r\nfoo\r\n:123\r\n*1\r\n$6\r\nnested\r\n
        String expected = "*3\r\n$3\r\nfoo\r\n:123\r\n*1\r\n$6\r\nnested\r\n";
        assertEquals(expected, output);
        out.release();
    }
    
    @Test
    public void testEncodeNullBulkString() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespEncoder());
        
        byte[] encoded = Resp.bulkString((String)null);
        assertTrue(channel.writeOutbound(encoded));
        
        ByteBuf out = channel.readOutbound();
        assertEquals("$-1\r\n", out.toString(StandardCharsets.UTF_8));
        out.release();
    }
    
    @Test
    public void testEncodeError() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespEncoder());
        
        byte[] encoded = Resp.error("ERR unknown command");
        assertTrue(channel.writeOutbound(encoded));
        
        ByteBuf out = channel.readOutbound();
        assertEquals("-ERR unknown command\r\n", out.toString(StandardCharsets.UTF_8));
        out.release();
    }
}
