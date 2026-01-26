package core.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Encodes Java Objects (Strings, Integers, Lists, byte[]) into RESP format.
 */
public class NettyRespEncoder extends MessageToByteEncoder<Object> {
    
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (msg instanceof ByteBuf) {
            // Already encoded bytes (raw)
            out.writeBytes((ByteBuf) msg);
        } else if (msg instanceof byte[]) {
             out.writeBytes((byte[]) msg);
        } else if (msg instanceof String) {
             // If we receive a raw String, write it as bytes (handling text response mode)
             out.writeBytes(((String)msg).getBytes(StandardCharsets.UTF_8));
        } else {
             // Fallback
        }
    }
}
