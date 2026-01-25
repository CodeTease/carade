package core.protocol.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import net.jqwik.api.*;

public class RespParserPropertyTest {

    @Property
    void parsingShouldNotCrash(@ForAll byte[] bytes) {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyRespDecoder());
        try {
            channel.writeInbound(Unpooled.wrappedBuffer(bytes));
        } catch (Exception e) {
            // Decoding errors are expected, but checks for specific runtime exceptions like NPE or OutOfBounds might be useful if we want strict robustness.
            // For now, we ensure it doesn't crash the JVM or hang.
        }
    }
}
