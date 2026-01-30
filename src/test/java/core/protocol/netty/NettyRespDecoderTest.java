package core.protocol.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class NettyRespDecoderTest {

    @Test
    public void testFragmentedCommand() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        
        channel.writeInbound(Unpooled.wrappedBuffer("*3\r\n$3\r\nSE".getBytes(StandardCharsets.UTF_8)));
        assertNull(channel.readInbound());
        
        channel.writeInbound(Unpooled.wrappedBuffer("T\r\n$3\r\nkey\r\n$3\r".getBytes(StandardCharsets.UTF_8)));
        assertNull(channel.readInbound());
        
        channel.writeInbound(Unpooled.wrappedBuffer("\nval\r\n".getBytes(StandardCharsets.UTF_8)));
        
        Object msg = channel.readInbound();
        assertNotNull(msg);
        assertTrue(msg instanceof List);
        List<byte[]> args = (List<byte[]>) msg;
        assertEquals(3, args.size());
        assertArrayEquals("SET".getBytes(StandardCharsets.UTF_8), args.get(0));
        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), args.get(1));
        assertArrayEquals("val".getBytes(StandardCharsets.UTF_8), args.get(2));
    }
}
