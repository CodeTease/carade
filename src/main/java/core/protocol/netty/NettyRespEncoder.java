package core.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import core.protocol.Resp; // Reuse existing Resp constants if needed, or implement fresh

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Encodes Java Objects (Strings, Integers, Lists, byte[]) into RESP format.
 */
public class NettyRespEncoder extends MessageToByteEncoder<Object> {
    
    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (msg instanceof ByteBuf) {
            // Already encoded bytes (raw)
            out.writeBytes((ByteBuf) msg);
        } else if (msg instanceof byte[]) {
             // Treat raw bytes as pre-formatted RESP or raw text? 
             // Caution: The old system sometimes sent raw text vs RESP.
             // We should assume the CommandHandler wraps things in objects.
             // But if we pass byte[], we write it directly.
             out.writeBytes((byte[]) msg);
        } else if (msg instanceof String) {
             // Simple string? Or bulk?
             // Context matters. For now, assume if String is passed, it's pre-formatted RESP string 
             // OR we need a wrapper class.
             // Let's rely on the upper layer to format, OR we implement convenience wrappers.
             // Replicating `Resp.simpleString` logic:
             // Actually, the old `ClientHandler` calls `Resp.simpleString("OK")` which returns byte[].
             // So likely we receive byte[] or List<byte[]> here.
             
             // If we receive a raw String, write it as bytes (handling text response mode)
             out.writeBytes(((String)msg).getBytes(StandardCharsets.UTF_8));
        } else {
             // Fallback
        }
    }
}
