package core.protocol;

import core.protocol.netty.NettyRespDecoder;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ProtocolFuzzingTest {

    @Test
    public void testMalformedResp() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        
        // Send garbage
        String garbage = "*2\r\n$3\r\nSET\r\n$garbage\r\n"; // Invalid length string
        channel.writeInbound(Unpooled.copiedBuffer(garbage, StandardCharsets.UTF_8));
        
        // The decoder calls ctx.close() on format error (NumberFormatException)
        assertFalse(channel.isOpen(), "Channel should be closed on invalid integer length");
    }
    
    @Test
    public void testPartialPackets() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        
        channel.writeInbound(Unpooled.copiedBuffer("*2\r\n$3\r\nSET\r\n", StandardCharsets.UTF_8));
        // Should wait for more
        Object msg = channel.readInbound();
        assertNull(msg);
        
        channel.writeInbound(Unpooled.copiedBuffer("$1\r\nk\r\n", StandardCharsets.UTF_8));
        msg = channel.readInbound();
        assertNotNull(msg);
        assertTrue(msg instanceof List);
        assertEquals(2, ((List)msg).size());
    }
    
    @Test
    public void testMissingCRLF() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        
        // Send without \r\n
        channel.writeInbound(Unpooled.copiedBuffer("*1\r$4\rPING\r", StandardCharsets.UTF_8)); 
        
        Object msg = channel.readInbound();
        assertNull(msg, "Should not produce message on incomplete delimiter");
        
        // Finish it (assuming flexible decoder or strict)
        channel.writeInbound(Unpooled.copiedBuffer("\n", StandardCharsets.UTF_8)); // complete the *1 line
        
        // Expect connection close due to invalid protocol
        assertFalse(channel.isOpen(), "Channel should close on invalid protocol");
    }
}
