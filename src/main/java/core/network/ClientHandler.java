package core.network;

import core.Carade;
import core.Config;
import core.PubSub;
import core.commands.Command;
import core.commands.CommandRegistry;
import core.db.DataType;
import core.db.ValueEntry;
import core.protocol.Resp;
import core.server.WriteSequencer;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import core.structs.BloomFilter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientHandler extends ChannelInboundHandlerAdapter implements PubSub.Subscriber {
    private ChannelHandlerContext ctx;
    private String clientName = null;
    private Config.User currentUser = null; // null = not authenticated
    public int dbIndex = 0; // Current DB index
    
    public int getDbIndex() {
        return dbIndex;
    }
    
    public boolean isSubscribed = false; // Accessible by Carade (hacky)
    private boolean isMonitor = false;
    private boolean currentIsResp = true; // Netty decoder implies RESP mode, but we might support legacy text
    
    // Transaction State
    private boolean isInTransaction = false;
    private volatile boolean transactionDirty = false;
    private Set<String> watching = new HashSet<>();
    private List<List<byte[]>> transactionQueue = new ArrayList<>();
    private OutputStream captureBuffer = null; // For capturing output during transactions

    public boolean isInTransaction() {
        return isInTransaction;
    }

    public void setInTransaction(boolean inTransaction) {
        isInTransaction = inTransaction;
    }
    
    public boolean isTransactionDirty() {
        return transactionDirty;
    }
    
    public void setTransactionDirty(boolean dirty) {
        transactionDirty = dirty;
    }
    
    public List<List<byte[]>> getTransactionQueue() {
        return transactionQueue;
    }
    
    public void clearTransactionQueue() {
        transactionQueue.clear();
    }
    
    public void setCaptureBuffer(OutputStream out) {
        this.captureBuffer = out;
    }
    
    public void setCurrentUser(Config.User user) {
        this.currentUser = user;
    }

    public ClientHandler() { 
        // No socket passed in constructor for Netty
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        Carade.activeConnections.incrementAndGet();
        Carade.connectedClients.add(this);
        super.channelActive(ctx);
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // cause.printStackTrace();
        ctx.close();
    }
    
    private void cleanup() {
        Carade.pubSub.unsubscribeAll(this);
        core.replication.ReplicationManager.getInstance().removeReplica(this);
        Carade.monitors.remove(this);
        unwatchAll();
        Carade.activeConnections.decrementAndGet();
        Carade.connectedClients.remove(this);
    }

    public void setMonitor(boolean isMonitor) {
        this.isMonitor = isMonitor;
    }

    public boolean isMonitor() {
        return isMonitor;
    }

    public void setClientName(String name) {
        this.clientName = name;
    }

    public String getClientName() {
        return clientName;
    }

    public void markDirty() {
        this.transactionDirty = true;
    }

    public void unwatchAll() {
        if (watching.isEmpty()) return;
        for (String key : watching) {
            Carade.watchers.computeIfPresent(key, (k, list) -> {
                list.remove(this);
                return list.isEmpty() ? null : list;
            });
        }
        watching.clear();
    }
    
    public void addWatch(String key) {
        watching.add(key);
        Carade.watchers.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(this);
    }

    // New send method for Netty
    public synchronized void send(boolean isResp, Object data, String textData) {
        if (captureBuffer != null) {
            try {
                if (isResp && data != null) {
                    if (data instanceof byte[]) captureBuffer.write((byte[]) data);
                    else if (data instanceof ByteBuf) {
                         ByteBuf b = (ByteBuf) data;
                         byte[] arr = new byte[b.readableBytes()];
                         b.getBytes(b.readerIndex(), arr);
                         captureBuffer.write(arr);
                    }
                } else if (!isResp && textData != null) {
                    captureBuffer.write((textData + "\n").getBytes(StandardCharsets.UTF_8));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        if (ctx == null) return;
        
        if (isResp) {
            if (data != null) ctx.writeAndFlush(data);
        } else {
             if (textData != null) ctx.writeAndFlush(textData + "\n");
        }
    }

    // Deprecated / Adapted signature for existing commands
    public void send(OutputStream out, boolean isResp, byte[] respData, String textData) {
        if (out instanceof ByteArrayOutputStream) {
            try {
                if (isResp && respData != null) out.write(respData);
                else if (!isResp && textData != null) out.write((textData + "\n").getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {}
            return;
        }

        send(isResp, respData, textData);
    }
    
    // For ExecCommand to write raw buffer
    public void writeDirect(Object msg) {
        if (ctx != null) ctx.writeAndFlush(msg);
    }
    
    public void scheduleTimeout(Runnable task, long delayMs) {
        if (ctx != null) ctx.executor().schedule(task, delayMs, TimeUnit.MILLISECONDS);
    }
    
    // Helper for Blocking commands
    public void scheduleTimeout(Carade.BlockingRequest bReq, long delayMs) {
         scheduleTimeout(() -> {
            if (!bReq.future.isDone()) {
                bReq.future.cancel(true); // Will trigger whenComplete with CancellationException usually, or we handle it
                sendNull();
            }
        }, delayMs);
    }

    public void sendResponse(byte[] respData, String textData) {
        send(currentIsResp, respData, textData);
    }
    
    public void sendSimpleString(String msg) {
        send(currentIsResp, Resp.simpleString(msg), msg);
    }

    public void sendError(String msg) {
        send(currentIsResp, Resp.error(msg), "(error) " + msg);
    }

    public void sendInteger(long i) {
        send(currentIsResp, Resp.integer(i), "(integer) " + i);
    }
    
    public void sendNull() {
        send(currentIsResp, Resp.bulkString((byte[])null), "(nil)");
    }
    
    public void sendBulkString(String s) {
        send(currentIsResp, Resp.bulkString(s), s == null ? "(nil)" : "\"" + s + "\"");
    }
    
    public void sendArray(List<byte[]> list) {
        if (currentIsResp) {
            send(true, Resp.array(list), null);
        } else {
            StringBuilder sb = new StringBuilder();
            if (list == null || list.isEmpty()) {
                sb.append("(empty list or set)");
            } else {
                for (int i = 0; i < list.size(); i++) {
                    sb.append(i + 1).append(") \"").append(new String(list.get(i), StandardCharsets.UTF_8)).append("\"\n");
                }
            }
            send(false, null, sb.toString().trim());
        }
    }
    
    public void sendMixedArray(List<Object> list) {
         if (currentIsResp) {
             send(true, Resp.mixedArray(list), null);
         } else {
             send(false, null, mixedArrayToString(list, 0).trim());
         }
    }

    public void executeWrite(Runnable dbOp, String cmdName, Object... args) {
        List<byte[]> parts = new ArrayList<>();
        parts.add(cmdName.getBytes(StandardCharsets.UTF_8));
        for (Object arg : args) {
            if (arg instanceof String) {
                parts.add(((String) arg).getBytes(StandardCharsets.UTF_8));
            } else if (arg instanceof byte[]) {
                parts.add((byte[]) arg);
            } else {
                parts.add(String.valueOf(arg).getBytes(StandardCharsets.UTF_8));
            }
        }
        byte[] serializedCmd = Resp.array(parts);
        WriteSequencer.getInstance().executeWrite(dbOp, serializedCmd);
    }
    
    private String formatDouble(double d) {
        if (d == (long) d) return String.format("%d", (long) d);
        else return String.format("%s", d);
    }

    private String mixedArrayToString(List<Object> list, int level) {
        if (list == null || list.isEmpty()) return "(empty list or set)";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            Object o = list.get(i);
            for(int j=0; j<level; j++) sb.append("  ");
            
            sb.append(i + 1).append(") ");
            if (o instanceof byte[]) {
                sb.append("\"").append(new String((byte[]) o, StandardCharsets.UTF_8)).append("\"\n");
            } else if (o instanceof Long || o instanceof Integer) {
                sb.append("(integer) ").append(o).append("\n");
            } else if (o instanceof List) {
                sb.append("\n").append(mixedArrayToString((List<Object>) o, level + 1));
            } else {
                sb.append(o).append("\n");
            }
        }
        return sb.toString();
    }

    private boolean isWriteCommand(String cmd) {
        return Arrays.asList("SET", "DEL", "LPUSH", "RPUSH", "LPOP", "RPOP", 
                             "HSET", "HDEL", "SADD", "SREM", "FLUSHALL", "FLUSHDB",
                             "HINCRBY", "SISMEMBER", "SCARD", 
                             "RENAME", "ZREM", "SETBIT", "INCR", "DECR", "EXPIRE", 
                             "MSET", "ZADD", "ZINCRBY", "RPOPLPUSH", "LTRIM", "BITOP", "PFADD",
                             "ZPOPMIN", "ZPOPMAX", "HSETNX", "HINCRBYFLOAT", "LMOVE", "BLMOVE", "UNLINK", "BF.ADD",
                             "SETEX", "PSETEX", "MSETNX", "GETDEL", "GETEX", "LPUSHX", "RPUSHX", "LSET", "HMSET", "HEXPIRE").contains(cmd);
    }
    
    private boolean isAdminCommand(String cmd) {
        return Arrays.asList("FLUSHALL", "DBSIZE").contains(cmd);
    }

    @Override
    public void send(String channel, String message, String pattern) {
        if (currentIsResp) {
            if (pattern != null) {
                List<byte[]> resp = new ArrayList<>();
                resp.add("pmessage".getBytes(StandardCharsets.UTF_8));
                resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                resp.add(channel.getBytes(StandardCharsets.UTF_8));
                resp.add(message.getBytes(StandardCharsets.UTF_8));
                send(true, Resp.array(resp), null);
            } else {
                List<byte[]> resp = new ArrayList<>();
                resp.add("message".getBytes(StandardCharsets.UTF_8));
                resp.add(channel.getBytes(StandardCharsets.UTF_8));
                resp.add(message.getBytes(StandardCharsets.UTF_8));
                send(true, Resp.array(resp), null);
            }
        } else {
            if (pattern != null) send(false, null, "[MSG][" + pattern + "] " + channel + ": " + message);
            else send(false, null, "[MSG] " + channel + ": " + message);
        }
    }

    @Override
    public boolean isResp() { return currentIsResp; }
    
    @Override
    public Object getId() { return this; }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof List) {
            List<byte[]> parts = (List<byte[]>) msg;
            handleCommand(parts);
        }
    }

    private void handleCommand(List<byte[]> parts) {
        if (parts.isEmpty()) return;
        Carade.totalCommands.incrementAndGet();

        long startTime = System.nanoTime();
        String cmd = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();
        
        boolean isResp = true; 
        this.currentIsResp = true;

        // Broadcast to Monitors (Redis format: timestamp [db lua/ip:port] "command" "args")
        if (!Carade.monitors.isEmpty() && !cmd.equals("AUTH") && !cmd.equals("QUIT")) {
            long now = System.currentTimeMillis();
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(java.util.Locale.US, "%.6f", now / 1000.0)).append(" [").append(dbIndex).append(" ").append(ctx != null && ctx.channel().remoteAddress() != null ? ctx.channel().remoteAddress() : "0.0.0.0:0").append("] ");
            sb.append("\"").append(cmd).append("\"");
            for (int i = 1; i < parts.size(); i++) {
                sb.append(" \"").append(new String(parts.get(i), StandardCharsets.UTF_8)).append("\"");
            }
            String logLine = sb.toString();
            for (ClientHandler monitor : Carade.monitors) {
                if (monitor != this) { // Don't echo to self if self is monitoring (unlikely but safe)
                    monitor.send(monitor.isResp(), Resp.simpleString(logLine), logLine);
                }
            }
        }

        // Handle Monitor mode blocking
        if (isMonitor) {
            if (cmd.equals("QUIT")) {
                ctx.close();
                return;
            }
            // Monitors can only run QUIT (or RESET in newer Redis, but we just ignore/block others)
            // Just return (block) or send error? Redis usually just blocks until QUIT.
            // But if we send error, it might break client expectation of stream. 
            // We just ignore other commands or treat them as part of the stream?
            // Redis doc: "In this mode, the server will not process any other command from this client, but QUIT."
            return; 
        }

        // Handle Subs commands in Sub mode
        if (isSubscribed) {
            if (cmd.equals("QUIT")) {
                ctx.close();
                return;
            }
            if (!Arrays.asList("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE").contains(cmd)) {
                 return; // Ignore
            }
        }

        if (currentUser == null && !cmd.equals("AUTH") && !cmd.equals("QUIT")) {
            sendError("NOAUTH Authentication required");
            return;
        }
        
        if (currentUser != null && !currentUser.canWrite && isWriteCommand(cmd)) {
             sendError("ERR permission denied");
             return;
        }
        if (currentUser != null && !currentUser.isAdmin && isAdminCommand(cmd)) {
             sendError("ERR permission denied");
             return;
        }

        try {
            // Handle Transactions Pre-check (Execution Logic moved to ExecCommand, but Dispatcher still needs to know about Queueing)
            if (isInTransaction && !cmd.equals("AUTH") && !cmd.equals("QUIT") && !cmd.equals("EXEC") && !cmd.equals("DISCARD") && !cmd.equals("WATCH")) {
                transactionQueue.add(parts);
                send(isResp, Resp.simpleString("QUEUED"), "QUEUED");
                return;
            }

            boolean needsWriteLock = isWriteCommand(cmd) || Arrays.asList("BGREWRITEAOF", "EXEC", "MULTI", "DISCARD").contains(cmd);
            
            // Note: EXEC needs write lock because it executes write commands
            // MULTI/DISCARD change internal state, but might be safe without global lock if state is local.
            // But we use global lock for simplicity in original code?
            // Original code: MULTI, DISCARD, EXEC were handled before lock logic.
            // Now they are Commands. We should treat them carefully.
            // Actually, `EXEC` executes commands which might need locks.
            // So executing `EXEC` under write lock is safe.
            // `MULTI`/`DISCARD` just change local boolean, so they are fast.

            if (needsWriteLock) {
                Carade.globalRWLock.writeLock().lock();
                try {
                    executeCommand(parts, null, isResp); 
                } finally {
                    Carade.globalRWLock.writeLock().unlock();
                }
            } else {
                Carade.globalRWLock.readLock().lock();
                try {
                    executeCommand(parts, null, isResp);
                } finally {
                    Carade.globalRWLock.readLock().unlock();
                }
            }
            
            if (cmd.equals("QUIT")) {
                // If QuitCommand wasn't executed or we want to ensure close
                // QuitCommand implementation might not have closed the ctx if it just sent OK.
                // But usually QUIT closes connection.
                // Let's rely on QuitCommand doing it or check here?
                // The switch case had `ctx.close()`.
                // Our QuitCommand sends OK.
                // We should probably check if channel is active or close it.
                // But let's leave it to Command if possible. 
                // Wait, QuitCommand in my impl just sent OK. It didn't close.
                // I should probably check here.
                ctx.close();
            }

        } catch (Exception e) { 
            sendError("ERR " + e.getMessage());
        } finally {
            long duration = System.nanoTime() - startTime;
            if (duration > 10_000_000) { // 10ms
                StringBuilder sb = new StringBuilder();
                sb.append(System.currentTimeMillis()).append(" ");
                sb.append(duration / 1000).append("us "); // microseconds
                for (byte[] part : parts) {
                    sb.append(new String(part, StandardCharsets.UTF_8)).append(" ");
                }
                Carade.slowLog.add(sb.toString().trim());
                while (Carade.slowLog.size() > Carade.SLOWLOG_MAX_LEN) {
                    Carade.slowLog.poll();
                }
            }
        }
    }
    
    public void executeCommand(List<byte[]> parts, OutputStream out, boolean isResp) throws IOException {
        String cmd = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();
        
        Command cmdObj = CommandRegistry.get(cmd);
        if (cmdObj != null) {
             cmdObj.execute(this, parts);
        } else {
             send(out, isResp, Resp.error("ERR unknown command"), "(error) ERR unknown command");
        }
    }

    public String getRemoteAddress() {
        if (ctx != null && ctx.channel().remoteAddress() != null) {
            return ctx.channel().remoteAddress().toString();
        }
        return "0.0.0.0:0";
    }

    public void close() {
        if (ctx != null) ctx.close();
    }
}
