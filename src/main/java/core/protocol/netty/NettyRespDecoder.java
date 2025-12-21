package core.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Netty decoder for RESP (Redis Serialization Protocol).
 * Replaces the custom Resp.parse() logic.
 */
public class NettyRespDecoder extends ByteToMessageDecoder {

    private enum State {
        READ_TYPE,
        READ_LINE_LENGTH,
        READ_BULK_LENGTH,
        READ_BULK_CONTENT
    }

    private State state = State.READ_TYPE;
    
    // Multi-bulk parsing state
    private int multiBulkLength = 0; // Number of elements expected in array
    private List<byte[]> currentArray = null; // Accumulating args
    
    // Bulk string parsing state
    private int currentBulkLength = 0; // Length of current bulk string

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        while (true) {
            if (state == State.READ_TYPE) {
                if (!in.isReadable()) return;
                
                // If we are not currently parsing an array, we expect a new command
                if (currentArray == null) {
                    // Peek first byte
                    byte type = in.readByte();
                    if (type == '*') {
                        state = State.READ_LINE_LENGTH;
                        multiBulkLength = 0;
                    } else {
                        // Inline command (e.g. PING\r\n) or simple string
                        // For simplicity in this refactor, we assume standard RESP Arrays for commands
                        // But legacy telnet-style commands might just be text lines.
                        // Let's handle simple inline commands if it's not *
                        in.readerIndex(in.readerIndex() - 1); // Reset
                        state = State.READ_LINE_LENGTH; // Treat as line to parse
                        multiBulkLength = -1; // Special flag for inline
                    }
                } else {
                    // We are inside an array, reading elements
                    // Expecting Bulk Strings ($) typically
                    byte type = in.readByte();
                    if (type == '$') {
                        state = State.READ_BULK_LENGTH;
                    } else if (type == ':') {
                        // Integer
                        // Not typical for commands but possible
                         state = State.READ_LINE_LENGTH;
                         // Handling logic...
                    } else {
                         // Unexpected for command args, likely inline or error
                         // For now assume strictly Bulk Strings for args
                         in.readerIndex(in.readerIndex() - 1);
                         state = State.READ_LINE_LENGTH; // Try to read line
                    }
                }
            }

            if (state == State.READ_LINE_LENGTH) {
                // Read until \r\n
                // ByteBufUtils or simple loop
                int eol = findEndOfLine(in);
                if (eol == -1) return; // Wait for more data

                int length = eol - in.readerIndex();
                String line = in.toString(in.readerIndex(), length, StandardCharsets.UTF_8);
                in.readerIndex(eol + 2); // Skip \r\n

                if (multiBulkLength == 0) {
                     // We just read *<count>
                     try {
                        int count = Integer.parseInt(line);
                        if (count <= 0) {
                            // Empty array
                            state = State.READ_TYPE;
                            return; 
                        }
                        multiBulkLength = count;
                        currentArray = new ArrayList<>(count);
                        state = State.READ_TYPE; // Go back to read elements
                     } catch (NumberFormatException e) {
                         // Error
                         ctx.close();
                         return;
                     }
                } else if (multiBulkLength == -1) {
                    // Inline command: "PING" or "SET key val"
                    String[] parts = line.split("\\s+");
                    List<byte[]> args = new ArrayList<>();
                    for (String part : parts) {
                        if (!part.isEmpty()) args.add(part.getBytes(StandardCharsets.UTF_8));
                    }
                    if (!args.isEmpty()) out.add(args);
                    state = State.READ_TYPE;
                    multiBulkLength = 0;
                    currentArray = null;
                    return; // Emitted one command
                } else {
                    // We are reading a line inside an array (maybe integer arg?)
                    // But usually we go via READ_BULK_LENGTH for $
                    // If we got here, it might be a simple string or integer arg
                    // Add to array
                    currentArray.add(line.getBytes(StandardCharsets.UTF_8));
                    checkArrayComplete(out);
                }
            }

            if (state == State.READ_BULK_LENGTH) {
                 int eol = findEndOfLine(in);
                 if (eol == -1) return;
                 
                 int length = eol - in.readerIndex();
                 String line = in.toString(in.readerIndex(), length, StandardCharsets.UTF_8);
                 in.readerIndex(eol + 2);
                 
                 try {
                     currentBulkLength = Integer.parseInt(line);
                     if (currentBulkLength == -1) {
                         // Null bulk string
                         currentArray.add(null);
                         checkArrayComplete(out);
                     } else {
                         state = State.READ_BULK_CONTENT;
                     }
                 } catch (NumberFormatException e) {
                     ctx.close();
                     return;
                 }
            }
            
            if (state == State.READ_BULK_CONTENT) {
                if (in.readableBytes() < currentBulkLength + 2) return; // Need content + \r\n
                
                byte[] content = new byte[currentBulkLength];
                in.readBytes(content);
                in.skipBytes(2); // Skip \r\n
                
                currentArray.add(content);
                checkArrayComplete(out);
            }
        }
    }
    
    private void checkArrayComplete(List<Object> out) {
        if (currentArray.size() == multiBulkLength) {
            out.add(currentArray);
            currentArray = null;
            multiBulkLength = 0;
            state = State.READ_TYPE;
        } else {
            state = State.READ_TYPE; // Read next element
        }
    }

    private int findEndOfLine(ByteBuf in) {
        int n = in.writerIndex();
        for (int i = in.readerIndex(); i < n; i++) {
            byte b = in.getByte(i);
            if (b == '\r' && i + 1 < n && in.getByte(i + 1) == '\n') {
                return i;
            }
        }
        return -1;
    }
}
