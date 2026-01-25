package core.protocol;

import core.protocol.netty.NettyRespDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class NettyRespDecoderTest {

    @Test
    public void testFragmentedPacket() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        
        // Command: SET key val
        // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n
        
        String part1 = "*3\r\n$3\r\nSE";
        String part2 = "T\r\n$3\r\nkey\r\n$3\r\nval\r\n";
        
        channel.writeInbound(Unpooled.copiedBuffer(part1, StandardCharsets.UTF_8));
        Object output = channel.readInbound();
        assertNull(output); // Incomplete
        
        channel.writeInbound(Unpooled.copiedBuffer(part2, StandardCharsets.UTF_8));
        output = channel.readInbound();
        assertNotNull(output);
        
        List<byte[]> args = (List<byte[]>) output;
        assertEquals(3, args.size());
        assertEquals("SET", new String(args.get(0)));
        assertEquals("key", new String(args.get(1)));
        assertEquals("val", new String(args.get(2)));
    }

    @Test
    public void testLargePayloadHeader() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        
        // *1\r\n$<big_num>\r\n...
        String header = "*1\r\n$1000000\r\n";
        channel.writeInbound(Unpooled.copiedBuffer(header, StandardCharsets.UTF_8));
        
        Object output = channel.readInbound();
        assertNull(output); // Waiting for content
        
        // We don't send 1MB actual data to keep test fast, but decoder should accept header state.
        // If we close now, it's fine.
        channel.finish();
    }
    
    @Test
    public void testMalformedInput() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        
        // Garbage with newline to trigger processing
        channel.writeInbound(Unpooled.copiedBuffer("NOT_RESP_FORMAT\r\n", StandardCharsets.UTF_8));
        
        // Logic says: if not '*', assume inline?
        // Inline logic: split by space.
        // "NOT_RESP_FORMAT" -> ["NOT_RESP_FORMAT"]
        
        Object output = channel.readInbound();
        assertNotNull(output);
        List<byte[]> args = (List<byte[]>) output;
        assertEquals("NOT_RESP_FORMAT", new String(args.get(0)));
        
        // Test real garbage that fails parsing (e.g. *ABC\r\n)
        EmbeddedChannel channel2 = new EmbeddedChannel(new NettyRespDecoder());
        channel2.writeInbound(Unpooled.copiedBuffer("*ABC\r\n", StandardCharsets.UTF_8));
        
        // Should close channel on NumberFormatException
        assertFalse(channel2.isOpen());
    }
}
