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
    public boolean isSubscribed = false; // Accessible by Carade (hacky)
    private boolean currentIsResp = true; // Netty decoder implies RESP mode, but we might support legacy text
    
    // Transaction State
    private boolean isInTransaction = false;
    private volatile boolean transactionDirty = false;
    private Set<String> watching = new HashSet<>();
    private List<List<byte[]>> transactionQueue = new ArrayList<>();

    // Compatibility shim for OutputStreams in PubSub/Commands
    // Since Netty is async and commands expect an OutputStream to write to immediately,
    // we need to adapt.
    // However, most commands just call client.send().
    // We will override send() to write to ctx.
    // For things that absolutely need OutputStream (like transaction buffering), we handle it locally.

    public ClientHandler() { 
        // No socket passed in constructor for Netty
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        Carade.activeConnections.incrementAndGet();
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
        unwatchAll();
        Carade.activeConnections.decrementAndGet();
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

    private void unwatchAll() {
        if (watching.isEmpty()) return;
        for (String key : watching) {
            Carade.watchers.computeIfPresent(key, (k, list) -> {
                list.remove(this);
                return list.isEmpty() ? null : list;
            });
        }
        watching.clear();
    }

    // New send method for Netty
    public synchronized void send(boolean isResp, Object data, String textData) {
        if (ctx == null) return;
        
        if (isResp) {
            if (data != null) ctx.writeAndFlush(data);
        } else {
             if (textData != null) ctx.writeAndFlush(textData + "\n");
        }
    }

    // Deprecated / Adapted signature for existing commands
    public void send(OutputStream out, boolean isResp, byte[] respData, String textData) {
        // 'out' is ignored in Netty mode, we use 'ctx'
        // If commands pass a specific stream (like buffer), they should probably not call this method 
        // but write to the stream directly. 
        // However, most commands call client.send(out, ...).
        // If out == this.dummyOutputStream, we write to Netty.
        // If out is a buffer (Transaction), we write to buffer.
        
        if (out instanceof ByteArrayOutputStream) {
            try {
                if (isResp && respData != null) out.write(respData);
                else if (!isResp && textData != null) out.write((textData + "\n").getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {}
            return;
        }

        send(isResp, respData, textData);
    }

    public void sendResponse(byte[] respData, String textData) {
        send(currentIsResp, respData, textData);
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
                             "MSET", "ZADD", "ZINCRBY", "RPOPLPUSH", "LTRIM", "BITOP", "PFADD").contains(cmd);
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
        
        // Simple heuristic for RESP vs Text (Netty Decoder sets this usually, but let's assume if we are here we are parsing RESP)
        // If we want to support raw text mode properly, we need to know from the decoder or infer.
        // For now, default true.
        boolean isResp = true; 
        this.currentIsResp = true;

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
            // Handle Transactions
            if (cmd.equals("MULTI")) {
                if (isInTransaction) {
                    sendError("ERR MULTI calls can not be nested");
                } else {
                    isInTransaction = true;
                    transactionQueue.clear();
                    send(isResp, Resp.simpleString("OK"), "OK");
                }
                return;
            } else if (cmd.equals("DISCARD")) {
                if (!isInTransaction) {
                    sendError("ERR DISCARD without MULTI");
                } else {
                    isInTransaction = false;
                    transactionQueue.clear();
                    unwatchAll();
                    transactionDirty = false;
                    send(isResp, Resp.simpleString("OK"), "OK");
                }
                return;
            } else if (cmd.equals("EXEC")) {
                if (!isInTransaction) {
                    sendError("ERR EXEC without MULTI");
                } else {
                    isInTransaction = false;
                    
                    if (transactionDirty) {
                        sendNull();
                        transactionQueue.clear();
                        transactionDirty = false;
                        unwatchAll();
                        return;
                    }

                    unwatchAll();
                    transactionDirty = false;
                    
                    if (transactionQueue.isEmpty()) {
                        sendArray(Collections.emptyList());
                    } else {
                        Carade.globalRWLock.writeLock().lock();
                        try {
                            // We need to capture output of executed commands
                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                            String header = "*" + transactionQueue.size() + "\r\n";
                            buffer.write(header.getBytes());
                            
                            for (List<byte[]> queuedCmd : transactionQueue) {
                                executeCommand(queuedCmd, buffer, isResp);
                            }
                            // Write raw bytes to Netty context
                            // We wrap in Unpooled.wrappedBuffer
                            ctx.writeAndFlush(Unpooled.wrappedBuffer(buffer.toByteArray()));
                        } finally {
                            Carade.globalRWLock.writeLock().unlock();
                        }
                    }
                }
                return;
            } else if (cmd.equals("WATCH")) {
                if (isInTransaction) {
                    sendError("ERR WATCH inside MULTI is not allowed");
                } else {
                    if (parts.size() < 2) {
                        sendError("usage: WATCH key [key ...]");
                    } else {
                        for (int i = 1; i < parts.size(); i++) {
                            String key = new String(parts.get(i), StandardCharsets.UTF_8);
                            watching.add(key);
                            Carade.watchers.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(this);
                        }
                        send(isResp, Resp.simpleString("OK"), "OK");
                    }
                }
                return;
            } else if (cmd.equals("UNWATCH")) {
                unwatchAll();
                transactionDirty = false;
                send(isResp, Resp.simpleString("OK"), "OK");
                return;
            }

            if (isInTransaction && !cmd.equals("AUTH") && !cmd.equals("QUIT")) {
                transactionQueue.add(parts);
                send(isResp, Resp.simpleString("QUEUED"), "QUEUED");
                return;
            }

            boolean needsWriteLock = isWriteCommand(cmd) || Arrays.asList("BGREWRITEAOF").contains(cmd);
            
            if (needsWriteLock) {
                Carade.globalRWLock.writeLock().lock();
                try {
                    executeCommand(parts, null, isResp); // null output stream means use default ctx
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
    
    // Adapted handleScan to not need explicit OutputStream unless forced
    private void handleScan(List<byte[]> parts, OutputStream out, boolean isResp, String cmd, String key) throws IOException {
         // Same logic, but using 'this.send'
         int cursorIdx = cmd.equals("SCAN") ? 1 : 2;
         if (parts.size() <= cursorIdx) {
             if (out != null) send(out, isResp, Resp.error("wrong number of arguments"), "error");
             else sendError("wrong number of arguments for '" + cmd.toLowerCase() + "' command");
             return;
         }
         
         String cursor = new String(parts.get(cursorIdx), StandardCharsets.UTF_8);
         String pattern = null;
         int count = 10;
         
         for (int i = cursorIdx + 1; i < parts.size(); i++) {
             String arg = new String(parts.get(i), StandardCharsets.UTF_8).toUpperCase();
             if (arg.equals("MATCH") && i + 1 < parts.size()) {
                 pattern = new String(parts.get(++i), StandardCharsets.UTF_8);
             } else if (arg.equals("COUNT") && i + 1 < parts.size()) {
                 try { count = Integer.parseInt(new String(parts.get(++i), StandardCharsets.UTF_8)); } catch (Exception e) {}
             }
         }
         
         java.util.regex.Pattern regex = null;
         if (pattern != null) {
             String r = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
             regex = java.util.regex.Pattern.compile(r);
         }
         
         Iterator<?> it;
         Carade.ScanCursor sc = null;
         
         if (cursor.equals("0")) {
             // New iterator
             if (cmd.equals("SCAN")) {
                 it = Carade.db.keySet(dbIndex).iterator();
             } else {
                 ValueEntry entry = Carade.db.get(dbIndex, key);
                 if (entry == null) {
                      if (out != null) send(out, isResp, Resp.array(Arrays.asList("0".getBytes(), Resp.array(Collections.emptyList()))), null);
                      else send(isResp, Resp.array(Arrays.asList("0".getBytes(StandardCharsets.UTF_8), Resp.array(Collections.emptyList()))), null);
                     return;
                 }
                 if (cmd.equals("HSCAN") && entry.type == DataType.HASH) {
                     it = ((ConcurrentHashMap<String, String>)entry.getValue()).entrySet().iterator();
                 } else if (cmd.equals("SSCAN") && entry.type == DataType.SET) {
                     it = ((Set<String>)entry.getValue()).iterator();
                 } else if (cmd.equals("ZSCAN") && entry.type == DataType.ZSET) {
                     it = ((CaradeZSet)entry.getValue()).scores.entrySet().iterator();
                 } else {
                     if (out != null) send(out, isResp, Resp.array(Arrays.asList("0".getBytes(), Resp.array(Collections.emptyList()))), null);
                     else send(isResp, Resp.array(Arrays.asList("0".getBytes(StandardCharsets.UTF_8), Resp.array(Collections.emptyList()))), null);
                     return;
                 }
             }
             sc = new Carade.ScanCursor(it, null); 
             String newCursor = String.valueOf(Carade.cursorIdGen.getAndIncrement());
             Carade.scanRegistry.put(newCursor, sc);
             cursor = newCursor;
         } else {
             sc = Carade.scanRegistry.get(cursor);
             if (sc == null) {
                  cursor = "0";
                  it = Collections.emptyIterator();
             } else {
                  it = sc.iterator;
                  sc.lastAccess = System.currentTimeMillis();
             }
         }
         
         List<byte[]> results = new ArrayList<>();
         int found = 0;
         while (it.hasNext() && found < count) {
             Object next = it.next();
             found++; 
             
             String k = null;
             
             if (cmd.equals("SCAN") || cmd.equals("SSCAN")) {
                 k = (String) next;
                 if (regex == null || regex.matcher(k).matches()) {
                     results.add(k.getBytes(StandardCharsets.UTF_8));
                 }
             } else if (cmd.equals("HSCAN")) {
                 Map.Entry<String, String> e = (Map.Entry<String, String>) next;
                 if (regex == null || regex.matcher(e.getKey()).matches()) {
                     results.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                     results.add(e.getValue().getBytes(StandardCharsets.UTF_8));
                 }
             } else if (cmd.equals("ZSCAN")) {
                 Map.Entry<String, Double> e = (Map.Entry<String, Double>) next;
                 if (regex == null || regex.matcher(e.getKey()).matches()) {
                     results.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                     String s = String.valueOf(e.getValue());
                     if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                     results.add(s.getBytes(StandardCharsets.UTF_8));
                 }
             }
         }
         
         if (!it.hasNext()) {
             Carade.scanRegistry.remove(cursor);
             cursor = "0";
         }
         
         if (isResp) {
             List<byte[]> outer = new ArrayList<>();
             outer.add(cursor.getBytes(StandardCharsets.UTF_8));
             outer.add(Resp.array(results));
             if (out != null) send(out, true, Resp.array(outer), null);
             else send(true, Resp.array(outer), null);
         } else {
             StringBuilder sb = new StringBuilder();
             sb.append("1) \"").append(cursor).append("\"\n");
             sb.append("2) ");
             for (int i=0; i<results.size(); i++) {
                 sb.append(i==0 ? "" : "\n   ").append(i+1).append(") \"").append(new String(results.get(i), StandardCharsets.UTF_8)).append("\"");
             }
             if (out != null) send(out, false, null, sb.toString());
             else send(false, null, sb.toString());
         }
    }
    
    private void executeCommand(List<byte[]> parts, OutputStream out, boolean isResp) throws IOException {
        String cmd = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();
        
        Command cmdObj = CommandRegistry.get(cmd);
        if (cmdObj != null) {
             if (out != null) {
                 // Hack to support transactions needing ByteArrayOutputStream
                 // We pass 'out' to client.send(out, ...)
                 // But Command implementation calls client.sendResponse() which uses 'this.outStream' (now gone) or 'send(out, ...)'
                 // We need to ensure Command logic uses the passed 'out' or we redirect.
                 // Since we cannot change Command code easily (it uses client.sendResponse usually),
                 // we might need to rely on the fact that existing commands call 'client.send(out...)'?
                 // Checking 'Command.java' would be good. 
                 // Assuming existing commands call 'client.send(out, ...)' or 'client.sendResponse(...)'?
                 // The old ClientHandler had 'this.outStream'. 
                 // If commands use client.sendResponse(), it uses default stream.
                 // For transactions, we need to redirect that default stream.
                 // We can use a ThreadLocal or a field since ClientHandler is per-connection (single threaded in Netty event loop usually).
             }
             cmdObj.execute(this, parts);
             return;
        }
        
        // Fallback to switch case
        switch (cmd) {
            case "SCAN":
                handleScan(parts, out, isResp, "SCAN", null);
                break;
            case "HSCAN":
            case "SSCAN":
            case "ZSCAN":
                if (parts.size() < 2) {
                    if (out!=null) send(out, isResp, Resp.error("usage"), "error");
                    else sendError("usage: " + cmd + " key cursor [MATCH pattern] [COUNT count]");
                } else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    handleScan(parts, out, isResp, cmd, key);
                }
                break;

            case "AUTH":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: AUTH [user] password"), "(error) usage: AUTH [user] password");
                else {
                    String user = "default";
                    String pass = "";
                    if (parts.size() == 2) {
                        pass = new String(parts.get(1), StandardCharsets.UTF_8);
                    } else {
                        user = new String(parts.get(1), StandardCharsets.UTF_8);
                        pass = new String(parts.get(2), StandardCharsets.UTF_8);
                    }
                    
                    Config.User u = Carade.config.users.get(user);
                    if (u != null && u.password.equals(pass)) {
                        currentUser = u;
                        send(out, isResp, Resp.simpleString("OK"), "OK");
                    } else {
                        send(out, isResp, Resp.error("WRONGPASS invalid username-password pair"), "(error) WRONGPASS invalid username-password pair");
                    }
                }
                break;

            case "EXISTS":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: EXISTS key"), "(error) usage: EXISTS key");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null) {
                         send(out, isResp, Resp.integer(0), "(integer) 0");
                    } else {
                         send(out, isResp, Resp.integer(1), "(integer) 1");
                    }
                }
                break;
            
            case "TYPE":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: TYPE key"), "(error) usage: TYPE key");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null) {
                         send(out, isResp, Resp.simpleString("none"), "none");
                    } else {
                         send(out, isResp, Resp.simpleString(entry.type.name().toLowerCase()), entry.type.name().toLowerCase());
                    }
                }
                break;

            case "RENAME":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: RENAME key newkey"), "(error) usage: RENAME key newkey");
                else {
                    String oldKey = new String(parts.get(1), StandardCharsets.UTF_8);
                    String newKey = new String(parts.get(2), StandardCharsets.UTF_8);
                    
                    final int[] success = {0};
                    executeWrite(() -> {
                        ValueEntry val = Carade.db.remove(dbIndex, oldKey);
                        if (val != null) {
                            Carade.db.put(dbIndex, newKey, val);
                            Carade.notifyWatchers(oldKey);
                            Carade.notifyWatchers(newKey);
                            success[0] = 1;
                        }
                    }, "RENAME", oldKey, newKey);

                    if (success[0] == 0) {
                        send(out, isResp, Resp.error("ERR no such key"), "(error) ERR no such key");
                    } else {
                        send(out, isResp, Resp.simpleString("OK"), "OK");
                    }
                }
                break;

            case "SETBIT":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: SETBIT key offset value"), "(error) usage: SETBIT key offset value");
                else {
                    Carade.performEvictionIfNeeded();
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    final int[] oldBit = {0};
                    try {
                        int offset = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                        int val = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                        String offsetStr = new String(parts.get(2), StandardCharsets.UTF_8);
                        String valStr = new String(parts.get(3), StandardCharsets.UTF_8);

                        if (val != 0 && val != 1) {
                            send(out, isResp, Resp.error("ERR bit is not an integer or out of range"), "(error) ERR bit is not an integer or out of range");
                        } else if (offset < 0) {
                            send(out, isResp, Resp.error("ERR bit offset is not an integer or out of range"), "(error) ERR bit offset is not an integer or out of range");
                        } else {
                            executeWrite(() -> {
                                Carade.db.getStore(dbIndex).compute(key, (k, v) -> {
                                    byte[] bytes;
                                    if (v == null) bytes = new byte[0];
                                    else if (v.type != DataType.STRING) throw new RuntimeException("WRONGTYPE");
                                    else bytes = (byte[]) v.getValue();
                                    
                                    int byteIndex = offset / 8;
                                    int bitIndex = 7 - (offset % 8);
                                    
                                    if (byteIndex < bytes.length) {
                                        oldBit[0] = (bytes[byteIndex] >> bitIndex) & 1;
                                    } else {
                                        oldBit[0] = 0;
                                    }
                                    
                                    if (byteIndex >= bytes.length) {
                                        byte[] newBytes = new byte[byteIndex + 1];
                                        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                                        bytes = newBytes;
                                    }
                                    
                                    if (val == 1) bytes[byteIndex] |= (1 << bitIndex);
                                    else bytes[byteIndex] &= ~(1 << bitIndex);
                                    
                                    ValueEntry newV = new ValueEntry(bytes, DataType.STRING, -1);
                                    if (v != null) newV.expireAt = v.expireAt;
                                    newV.touch();
                                    return newV;
                                });
                                Carade.notifyWatchers(key);
                            }, "SETBIT", key, offsetStr, valStr);
                            
                            send(out, isResp, Resp.integer(oldBit[0]), "(integer) " + oldBit[0]);
                        }
                    } catch (NumberFormatException e) {
                        send(out, isResp, Resp.error("ERR bit offset is not an integer or out of range"), "(error) ERR bit offset is not an integer or out of range");
                    } catch (RuntimeException e) {
                        String msg = e.getMessage();
                        if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                            send(out, isResp, Resp.error(msg), "(error) " + msg);
                        else throw e;
                    }
                }
                break;
            
            case "GETBIT":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: GETBIT key offset"), "(error) usage: GETBIT key offset");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    try {
                        int offset = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                        ValueEntry entry = Carade.db.get(dbIndex, key);
                        if (entry == null) {
                            send(out, isResp, Resp.integer(0), "(integer) 0");
                        } else if (entry.type != DataType.STRING) {
                            send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                        } else {
                            byte[] bytes = (byte[]) entry.getValue();
                            int byteIndex = offset / 8;
                            int bitIndex = 7 - (offset % 8);
                            
                            int bit = 0;
                            if (byteIndex < bytes.length) {
                                bit = (bytes[byteIndex] >> bitIndex) & 1;
                            }
                            send(out, isResp, Resp.integer(bit), "(integer) " + bit);
                        }
                    } catch (NumberFormatException e) {
                        send(out, isResp, Resp.error("ERR bit offset is not an integer or out of range"), "(error) ERR bit offset is not an integer or out of range");
                    }
                }
                break;

            case "MSET":
                if (parts.size() < 3 || (parts.size() - 1) % 2 != 0) {
                    send(out, isResp, Resp.error("wrong number of arguments for 'mset' command"), "(error) usage: MSET key value [key value ...]");
                } else {
                    Carade.performEvictionIfNeeded();
                    
                    Object[] logArgs = new Object[parts.size() - 1];
                    for(int i=1; i<parts.size(); i++) {
                        if ((i-1) % 2 == 0) logArgs[i-1] = new String(parts.get(i), StandardCharsets.UTF_8);
                        else logArgs[i-1] = parts.get(i);
                    }

                    executeWrite(() -> {
                        for (int i = 1; i < parts.size(); i += 2) {
                            String key = new String(parts.get(i), StandardCharsets.UTF_8);
                            byte[] val = parts.get(i + 1);
                            Carade.db.put(dbIndex, key, new ValueEntry(val, DataType.STRING, -1));
                            Carade.notifyWatchers(key);
                        }
                    }, "MSET", logArgs);

                    send(out, isResp, Resp.simpleString("OK"), "OK");
                }
                break;

            case "MGET":
                if (parts.size() < 2) {
                    send(out, isResp, Resp.error("wrong number of arguments for 'mget' command"), "(error) wrong number of arguments for 'mget' command");
                } else {
                    if (isResp) {
                        List<byte[]> results = new ArrayList<>();
                        for (int i = 1; i < parts.size(); i++) {
                            String key = new String(parts.get(i), StandardCharsets.UTF_8);
                            ValueEntry entry = Carade.db.get(dbIndex, key);
                            if (entry == null || entry.type != DataType.STRING) {
                                results.add(null);
                            } else {
                                results.add((byte[]) entry.getValue());
                            }
                        }
                        send(out, true, Resp.array(results), null);
                    } else {
                        List<String> results = new ArrayList<>();
                        for (int i = 1; i < parts.size(); i++) {
                            String key = new String(parts.get(i), StandardCharsets.UTF_8);
                            ValueEntry entry = Carade.db.get(dbIndex, key);
                            if (entry == null || entry.type != DataType.STRING) {
                                results.add(null);
                            } else {
                                results.add(new String((byte[]) entry.getValue(), StandardCharsets.UTF_8));
                            }
                        }
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < results.size(); i++) {
                            String val = results.get(i);
                            sb.append((i+1) + ") " + (val == null ? "(nil)" : "\"" + val + "\"") + "\n");
                        }
                        send(out, false, null, sb.toString().trim());
                    }
                }
                break;

            case "TTL":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: TTL key"), "(error) usage: TTL key");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null) send(out, isResp, Resp.integer(-2), "(integer) -2");
                    else if (entry.expireAt == -1) send(out, isResp, Resp.integer(-1), "(integer) -1");
                    else {
                        long ttl = (entry.expireAt - System.currentTimeMillis()) / 1000;
                        if (ttl < 0) {
                            Carade.db.remove(dbIndex, key);
                            send(out, isResp, Resp.integer(-2), "(integer) -2");
                        } else {
                            send(out, isResp, Resp.integer(ttl), "(integer) " + ttl);
                        }
                    }
                }
                break;

            case "EXPIRE":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: EXPIRE key seconds"), "(error) usage: EXPIRE key seconds");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    try {
                        long seconds = Long.parseLong(new String(parts.get(2), StandardCharsets.UTF_8));
                        long expireAt = System.currentTimeMillis() + (seconds * 1000);
                        
                        final int[] ret = {0};
                        
                        // Transform EXPIRE to PEXPIREAT (absolute time) for AOF/Replica
                        executeWrite(() -> {
                            Carade.db.getStore(dbIndex).computeIfPresent(key, (k, v) -> {
                                v.expireAt = expireAt;
                                ret[0] = 1;
                                return v;
                            });
                        }, "PEXPIREAT", key, String.valueOf(expireAt));

                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                    } catch (NumberFormatException e) {
                        send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                    }
                }
                break;

            case "KEYS":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: KEYS pattern"), "(error) usage: KEYS pattern");
                else {
                    String pattern = new String(parts.get(1), StandardCharsets.UTF_8);
                    List<byte[]> keys = new ArrayList<>();
                    List<String> keyStrings = new ArrayList<>();
                    if (pattern.equals("*")) {
                        for(String k : Carade.db.keySet(dbIndex)) {
                            keys.add(k.getBytes(StandardCharsets.UTF_8));
                            keyStrings.add(k);
                        }
                    } else {
                        String regex = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
                        java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
                        for (String k : Carade.db.keySet(dbIndex)) {
                            if (p.matcher(k).matches()) {
                                keys.add(k.getBytes(StandardCharsets.UTF_8));
                                keyStrings.add(k);
                            }
                        }
                    }
                    if (isResp) send(out, true, Resp.array(keys), null);
                    else {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < keyStrings.size(); i++) sb.append((i+1) + ") \"" + keyStrings.get(i) + "\"\n");
                        send(out, false, null, sb.toString().trim());
                    }
                }
                break;

            case "INCR":
            case "DECR":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: "+cmd+" key"), "(error) usage: "+cmd+" key");
                else {
                    Carade.performEvictionIfNeeded();
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    final long[] ret = {0};
                    try {
                        executeWrite(() -> {
                            Carade.db.getStore(dbIndex).compute(key, (k, v) -> {
                                long val = 0;
                                if (v == null) {
                                    val = 0;
                                } else if (v.type != DataType.STRING) {
                                    throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                                } else {
                                    try {
                                        val = Long.parseLong(new String((byte[])v.getValue(), StandardCharsets.UTF_8));
                                    } catch (NumberFormatException e) {
                                        throw new RuntimeException("ERR value is not an integer or out of range");
                                    }
                                }
                                
                                if (cmd.equals("INCR")) val++; else val--;
                                ret[0] = val;
                                
                                ValueEntry newV = new ValueEntry(String.valueOf(val).getBytes(StandardCharsets.UTF_8), DataType.STRING, -1);
                                if (v != null) newV.expireAt = v.expireAt;
                                newV.touch();
                                return newV;
                            });
                            Carade.notifyWatchers(key);
                        }, cmd, key);
                        
                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                    } catch (RuntimeException e) {
                        String msg = e.getMessage();
                        if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                            send(out, isResp, Resp.error(msg), "(error) " + msg);
                        else throw e;
                    }
                }
                break;
            
            // --- LISTS ---
            case "LPUSH":
            case "RPUSH":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: "+cmd+" key value [value ...]"), "(error) usage: "+cmd+" key value [value ...]");
                else {
                    Carade.performEvictionIfNeeded();
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    try {
                        Object[] args = new Object[parts.size()-1];
                        for(int i=1; i<parts.size(); i++) args[i-1] = new String(parts.get(i), StandardCharsets.UTF_8);

                        executeWrite(() -> {
                            Carade.db.getStore(dbIndex).compute(key, (k, v) -> {
                                ConcurrentLinkedDeque<String> list;
                                if (v == null) {
                                    list = new ConcurrentLinkedDeque<>();
                                    v = new ValueEntry(list, DataType.LIST, -1);
                                } else if (v.type != DataType.LIST) {
                                    throw new RuntimeException("WRONGTYPE");
                                } else {
                                    list = (ConcurrentLinkedDeque<String>) v.getValue();
                                }
                                
                                for (int i = 2; i < parts.size(); i++) {
                                    String val = new String(parts.get(i), StandardCharsets.UTF_8);
                                    if (cmd.equals("LPUSH")) list.addFirst(val); else list.addLast(val);
                                }
                                
                                v.touch(); // Update LRU
                                return v;
                            });
                            Carade.notifyWatchers(key);
                            Carade.checkBlockers(key); // Notify waiters
                        }, cmd, args);
                        
                        ValueEntry v = Carade.db.get(dbIndex, key);
                        int size = (v != null && v.getValue() instanceof Deque) ? ((Deque)v.getValue()).size() : 0;
                        send(out, isResp, Resp.integer(size), "(integer) " + size);
                    } catch (RuntimeException e) {
                        send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                    }
                }
                break;
            
            case "BLPOP":
            case "BRPOP":
                // Blocking commands in Netty need care. 
                // We cannot block the event loop!
                // We should probably check if available, if not, add to waiters and return.
                // The BlockingRequest future callback needs to write to ctx.
                
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: "+cmd+" key [key ...] timeout"), "(error) usage: "+cmd+" key [key ...] timeout");
                else {
                    try {
                        double timeout = Double.parseDouble(new String(parts.get(parts.size()-1), StandardCharsets.UTF_8));
                        List<String> keys = new ArrayList<>();
                        for(int i=1; i<parts.size()-1; i++) keys.add(new String(parts.get(i), StandardCharsets.UTF_8));
                        
                        boolean served = false;
                        for (String k : keys) {
                            ValueEntry entry = Carade.db.get(dbIndex, k);
                            if (entry != null && entry.type == DataType.LIST) {
                                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.getValue();
                                if (!list.isEmpty()) {
                                    final String[] valRef = {null};
                                    final String finalKey = k;
                                    
                                    executeWrite(() -> {
                                        ValueEntry e = Carade.db.get(dbIndex, finalKey);
                                        if (e != null && e.type == DataType.LIST) {
                                            ConcurrentLinkedDeque<String> l = (ConcurrentLinkedDeque<String>) e.getValue();
                                            valRef[0] = cmd.equals("BLPOP") ? l.pollFirst() : l.pollLast();
                                            if (valRef[0] != null) {
                                                if (l.isEmpty()) Carade.db.remove(dbIndex, finalKey);
                                                Carade.notifyWatchers(finalKey);
                                            }
                                        }
                                    }, cmd.equals("BLPOP") ? "LPOP" : "RPOP", k);
                                    
                                    if (valRef[0] != null) {
                                        List<byte[]> resp = new ArrayList<>();
                                        resp.add(k.getBytes(StandardCharsets.UTF_8));
                                        resp.add(valRef[0].getBytes(StandardCharsets.UTF_8));
                                        send(out, isResp, Resp.array(resp), null);
                                        served = true;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        if (!served) {
                            // Async blocking
                            Carade.BlockingRequest bReq = new Carade.BlockingRequest(cmd.equals("BLPOP"));
                            for (String k : keys) {
                                Carade.blockingRegistry.computeIfAbsent(k, x -> new ConcurrentLinkedQueue<>()).add(bReq);
                            }
                            
                            // Attach callback to future
                            bReq.future.whenComplete((result, ex) -> {
                                 if (ex != null) {
                                     sendNull();
                                 } else {
                                     sendArray(result);
                                 }
                            });
                            
                            // Handle Timeout
                            if (timeout > 0) {
                                // Schedule timeout
                                ctx.executor().schedule(() -> {
                                    if (!bReq.future.isDone()) {
                                        bReq.future.cancel(true); // Will trigger whenComplete with CancellationException usually, or we handle it
                                        sendNull();
                                    }
                                }, (long)(timeout * 1000), TimeUnit.MILLISECONDS);
                            }
                        }
                    } catch (NumberFormatException e) {
                        send(out, isResp, Resp.error("ERR timeout is not a float or out of range"), "(error) ERR timeout is not a float or out of range");
                    }
                }
                break;

            case "RPOPLPUSH":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: RPOPLPUSH source destination"), "(error) usage: RPOPLPUSH source destination");
                else {
                    String source = new String(parts.get(1), StandardCharsets.UTF_8);
                    String destination = new String(parts.get(2), StandardCharsets.UTF_8);
                    
                    final String[] valRef = {null};
                    try {
                        executeWrite(() -> {
                            ValueEntry entry = Carade.db.get(dbIndex, source);
                            if (entry == null || entry.type != DataType.LIST) {
                                // Will handle send outside or allow null valRef
                            } else {
                                ConcurrentLinkedDeque<String> srcList = (ConcurrentLinkedDeque<String>) entry.getValue();
                                String val = srcList.pollLast();
                                if (val != null) {
                                    if (srcList.isEmpty()) Carade.db.remove(dbIndex, source);
                                    Carade.notifyWatchers(source);
                                    
                                    Carade.db.getStore(dbIndex).compute(destination, (k, v) -> {
                                        if (v == null) {
                                            ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                                            list.addFirst(val);
                                            return new ValueEntry(list, DataType.LIST, -1);
                                        } else if (v.type == DataType.LIST) {
                                            ((ConcurrentLinkedDeque<String>) v.getValue()).addFirst(val);
                                            return v;
                                        }
                                        throw new RuntimeException("WRONGTYPE");
                                    });
                                    Carade.notifyWatchers(destination);
                                    valRef[0] = val;
                                }
                            }
                        }, "RPOPLPUSH", source, destination);

                        if (valRef[0] != null) {
                            send(out, isResp, Resp.bulkString(valRef[0].getBytes(StandardCharsets.UTF_8)), valRef[0]);
                        } else {
                            ValueEntry e = Carade.db.get(dbIndex, source);
                            if (e != null && e.type != DataType.LIST) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                            else send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                        }
                    } catch (RuntimeException e) {
                        if (e.getMessage().equals("WRONGTYPE")) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                        else throw e;
                    }
                }
                break;

            case "LTRIM":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: LTRIM key start stop"), "(error) usage: LTRIM key start stop");
                else {
                     String key = new String(parts.get(1), StandardCharsets.UTF_8);
                     ValueEntry entry = Carade.db.get(dbIndex, key);
                     if (entry == null) {
                         send(out, isResp, Resp.simpleString("OK"), "OK");
                     } else if (entry.type != DataType.LIST) {
                         send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                     } else {
                         try {
                             int start = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                             int stop = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                             
                             executeWrite(() -> {
                                 ValueEntry e = Carade.db.get(dbIndex, key);
                                 if (e != null && e.type == DataType.LIST) {
                                     ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) e.getValue();
                                     int size = list.size();
                                     int s = start;
                                     int st = stop;
                                     
                                     if (s < 0) s += size;
                                     if (st < 0) st += size;
                                     if (s < 0) s = 0;
                                     
                                     if (s > st || s >= size) {
                                         list.clear();
                                         Carade.db.remove(dbIndex, key);
                                     } else {
                                         if (st >= size) st = size - 1;
                                         int keep = st - s + 1;
                                         int currentSize = list.size(); // Recalculate size just in case
                                         
                                         // Remove from head
                                         for (int i = 0; i < s; i++) list.pollFirst();
                                         
                                         // Remove from tail
                                         int removeTail = list.size() - keep; // remaining size - keep
                                         for (int i = 0; i < removeTail; i++) list.pollLast();
                                     }
                                     Carade.notifyWatchers(key);
                                 }
                             }, "LTRIM", key, String.valueOf(start), String.valueOf(stop));
                             
                             send(out, isResp, Resp.simpleString("OK"), "OK");
                         } catch (NumberFormatException e) {
                             send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                         }
                     }
                }
                break;

            case "LRANGE":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: LRANGE key start stop"), "(error) usage: LRANGE key start stop");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                    else if (entry.type != DataType.LIST) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                    else {
                        ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.getValue();
                        int size = list.size(); // Approximate size
                        int start = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                        int end = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                        
                        if (start < 0) start += size;
                        if (end < 0) end += size;
                        if (start < 0) start = 0;
                        
                        List<byte[]> sub = new ArrayList<>();
                        List<String> subStr = new ArrayList<>();
                        if (start <= end) {
                            Iterator<String> it = list.iterator();
                            int idx = 0;
                            while (it.hasNext() && idx <= end) {
                                String s = it.next();
                                if (idx >= start) {
                                    sub.add(s.getBytes(StandardCharsets.UTF_8));
                                    subStr.add(s);
                                }
                                idx++;
                            }
                        }
                        if (isResp) send(out, true, Resp.array(sub), null);
                        else {
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < subStr.size(); i++) sb.append((i+1) + ") \"" + subStr.get(i) + "\"\n");
                            send(out, false, null, sb.toString().trim());
                        }
                    }
                }
                break;

            // --- HASHES ---
            case "HGETALL":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: HGETALL key"), "(error) usage: HGETALL key");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.HASH) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                    else {
                        ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.getValue();
                        List<byte[]> flat = new ArrayList<>();
                        List<String> flatStr = new ArrayList<>();
                        for (Map.Entry<String, String> e : map.entrySet()) {
                            flat.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                            flat.add(e.getValue().getBytes(StandardCharsets.UTF_8));
                            flatStr.add(e.getKey());
                            flatStr.add(e.getValue());
                        }
                        if (isResp) send(out, true, Resp.array(flat), null);
                        else {
                            StringBuilder sb2 = new StringBuilder();
                            for (int i = 0; i < flatStr.size(); i++) {
                                    sb2.append((i+1) + ") \"" + flatStr.get(i) + "\"\n");
                            }
                            send(out, false, null, sb2.toString().trim());
                        }
                    }
                }
                break;
            
            case "HDEL":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: HDEL key field"), "(error) usage: HDEL key field");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String field = new String(parts.get(2), StandardCharsets.UTF_8);
                    final int[] ret = {0};
                    
                    executeWrite(() -> {
                        Carade.db.getStore(dbIndex).computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.HASH) {
                                ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
                                if (map.remove(field) != null) ret[0] = 1;
                                if (map.isEmpty()) return null;
                            }
                            return v;
                        });
                        if (ret[0] == 1) {
                            Carade.notifyWatchers(key);
                        }
                    }, "HDEL", key, field);

                    send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                }
                break;

            case "HINCRBY":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: HINCRBY key field increment"), "(error) usage: HINCRBY key field increment");
                else {
                    Carade.performEvictionIfNeeded();
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String field = new String(parts.get(2), StandardCharsets.UTF_8);
                    final long[] ret = {0};
                    try {
                        long incr = Long.parseLong(new String(parts.get(3), StandardCharsets.UTF_8));
                        String incrStr = new String(parts.get(3), StandardCharsets.UTF_8);
                        
                        executeWrite(() -> {
                            Carade.db.getStore(dbIndex).compute(key, (k, v) -> {
                                if (v == null) {
                                    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                                    map.put(field, String.valueOf(incr));
                                    ret[0] = incr;
                                    return new ValueEntry(map, DataType.HASH, -1);
                                } else if (v.type != DataType.HASH) {
                                    throw new RuntimeException("WRONGTYPE");
                                } else {
                                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
                                    map.compute(field, (f, val) -> {
                                        long oldVal = 0;
                                        if (val != null) {
                                            try { oldVal = Long.parseLong(val); } catch (Exception e) { throw new RuntimeException("ERR hash value is not an integer"); }
                                        }
                                        long newVal = oldVal + incr;
                                        ret[0] = newVal;
                                        return String.valueOf(newVal);
                                    });
                                    v.touch();
                                    return v;
                                }
                            });
                            Carade.notifyWatchers(key);
                        }, "HINCRBY", key, field, incrStr);
                        
                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                    } catch (NumberFormatException e) {
                            send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                    } catch (RuntimeException e) {
                            String msg = e.getMessage();
                            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                                send(out, isResp, Resp.error(msg), "(error) " + msg);
                            else throw e;
                    }
                }
                break;

            // --- SETS ---
            case "SADD":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: SADD key member"), "(error) usage: SADD key member");
                else {
                    Carade.performEvictionIfNeeded();
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String member = new String(parts.get(2), StandardCharsets.UTF_8);
                    final int[] ret = {0};
                    try {
                        executeWrite(() -> {
                            Carade.db.getStore(dbIndex).compute(key, (k, v) -> {
                                if (v == null) {
                                    Set<String> set = ConcurrentHashMap.newKeySet();
                                    set.add(member);
                                    ret[0] = 1;
                                    return new ValueEntry(set, DataType.SET, -1);
                                } else if (v.type != DataType.SET) {
                                    throw new RuntimeException("WRONGTYPE");
                                } else {
                                    Set<String> set = (Set<String>) v.getValue();
                                    if (set.add(member)) ret[0] = 1;
                                    else ret[0] = 0;
                                    v.touch();
                                    return v;
                                }
                            });
                            if (ret[0] == 1) Carade.notifyWatchers(key);
                        }, "SADD", key, member);
                        
                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                    } catch (RuntimeException e) {
                        send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                    }
                }
                break;
                
            case "SMEMBERS":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SMEMBERS key"), "(error) usage: SMEMBERS key");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.SET) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                    else {
                        Set<String> set = (Set<String>) entry.getValue();
                        List<byte[]> list = new ArrayList<>();
                        List<String> listStr = new ArrayList<>();
                        for(String s : set) {
                            list.add(s.getBytes(StandardCharsets.UTF_8));
                            listStr.add(s);
                        }
                        if (isResp) send(out, true, Resp.array(list), null);
                        else {
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < listStr.size(); i++) sb.append((i+1) + ") \"" + listStr.get(i) + "\"\n");
                            send(out, false, null, sb.toString().trim());
                        }
                    }
                }
                break;

            case "SREM":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: SREM key member"), "(error) usage: SREM key member");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String member = new String(parts.get(2), StandardCharsets.UTF_8);
                    final int[] ret = {0};
                    
                    executeWrite(() -> {
                        Carade.db.getStore(dbIndex).computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.SET) {
                                Set<String> set = (Set<String>) v.getValue();
                                if (set.remove(member)) ret[0] = 1;
                                if (set.isEmpty()) return null;
                            }
                            return v;
                        });
                        if (ret[0] == 1) {
                             Carade.notifyWatchers(key);
                        }
                    }, "SREM", key, member);

                    send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                }
                break;
            
            case "SISMEMBER":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: SISMEMBER key member"), "(error) usage: SISMEMBER key member");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String member = new String(parts.get(2), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.SET) send(out, isResp, Resp.integer(0), "(integer) 0");
                    else {
                        Set<String> set = (Set<String>) entry.getValue();
                        send(out, isResp, Resp.integer(set.contains(member) ? 1 : 0), "(integer) " + (set.contains(member) ? 1 : 0));
                    }
                }
                break;
                
            case "SCARD":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SCARD key"), "(error) usage: SCARD key");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.SET) send(out, isResp, Resp.integer(0), "(integer) 0");
                    else {
                        Set<String> set = (Set<String>) entry.getValue();
                        send(out, isResp, Resp.integer(set.size()), "(integer) " + set.size());
                    }
                }
                break;
                
            case "SINTER":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SINTER key [key ...]"), "(error) usage: SINTER key [key ...]");
                else {
                    String firstKey = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, firstKey);
                    if (entry == null || entry.type != DataType.SET) {
                         send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                    } else {
                         Set<String> result = new HashSet<>((Set<String>) entry.getValue());
                         for (int i = 2; i < parts.size(); i++) {
                             ValueEntry e = Carade.db.get(dbIndex, new String(parts.get(i), StandardCharsets.UTF_8));
                             if (e == null || e.type != DataType.SET) {
                                 result.clear();
                                 break;
                             }
                             result.retainAll((Set<String>) e.getValue());
                         }
                         
                         if (isResp) {
                             List<byte[]> resp = new ArrayList<>();
                             for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
                             send(out, true, Resp.array(resp), null);
                         }
                         else {
                             StringBuilder sb = new StringBuilder();
                             int i = 1;
                             for (String s : result) sb.append(i++).append(") \"").append(s).append("\"\n");
                             send(out, false, null, sb.toString().trim());
                         }
                    }
                }
                break;

            case "SUNION":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SUNION key [key ...]"), "(error) usage: SUNION key [key ...]");
                else {
                     Set<String> result = new HashSet<>();
                     for (int i = 1; i < parts.size(); i++) {
                         ValueEntry e = Carade.db.get(dbIndex, new String(parts.get(i), StandardCharsets.UTF_8));
                         if (e != null && e.type == DataType.SET) {
                             result.addAll((Set<String>) e.getValue());
                         }
                     }
                     if (isResp) {
                         List<byte[]> resp = new ArrayList<>();
                         for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
                         send(out, true, Resp.array(resp), null);
                     }
                     else {
                         StringBuilder sb = new StringBuilder();
                         int i = 1;
                         for (String s : result) sb.append(i++).append(") \"").append(s).append("\"\n");
                         send(out, false, null, sb.toString().trim());
                     }
                }
                break;

            case "SDIFF":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SDIFF key [key ...]"), "(error) usage: SDIFF key [key ...]");
                else {
                    String firstKey = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, firstKey);
                    Set<String> result = new HashSet<>();
                    if (entry != null && entry.type == DataType.SET) {
                        result.addAll((Set<String>) entry.getValue());
                    }
                    
                    for (int i = 2; i < parts.size(); i++) {
                         ValueEntry e = Carade.db.get(dbIndex, new String(parts.get(i), StandardCharsets.UTF_8));
                         if (e != null && e.type == DataType.SET) {
                             result.removeAll((Set<String>) e.getValue());
                         }
                    }
                    
                    if (isResp) {
                         List<byte[]> resp = new ArrayList<>();
                         for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
                         send(out, true, Resp.array(resp), null);
                    }
                    else {
                         StringBuilder sb = new StringBuilder();
                         int i = 1;
                         for (String s : result) sb.append(i++).append(") \"").append(s).append("\"\n");
                         send(out, false, null, sb.toString().trim());
                     }
                }
                break;

            // --- SORTED SETS ---
            case "ZADD":
                if (parts.size() < 4 || (parts.size() - 2) % 2 != 0) {
                        send(out, isResp, Resp.error("usage: ZADD key score member [score member ...]"), "(error) usage: ZADD key score member ...");
                } else {
                        Carade.performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        final int[] addedCount = {0};
                        try {
                            String[] args = new String[parts.size()-1];
                            for(int i=1; i<parts.size(); i++) args[i-1] = new String(parts.get(i), StandardCharsets.UTF_8);
                            
                            executeWrite(() -> {
                                Carade.db.getStore(dbIndex).compute(key, (k, v) -> {
                                    CaradeZSet zset;
                                    if (v == null) {
                                        zset = new CaradeZSet();
                                        v = new ValueEntry(zset, DataType.ZSET, -1);
                                    } else if (v.type != DataType.ZSET) {
                                        throw new RuntimeException("WRONGTYPE");
                                    } else {
                                        zset = (CaradeZSet) v.getValue();
                                    }
                                    
                                    for (int i = 2; i < parts.size(); i += 2) {
                                        try {
                                            double score = Double.parseDouble(new String(parts.get(i), StandardCharsets.UTF_8));
                                            String member = new String(parts.get(i+1), StandardCharsets.UTF_8);
                                            addedCount[0] += zset.add(score, member);
                                        } catch (Exception ex) {}
                                    }
                                    v.touch();
                                    return v;
                                });
                                Carade.notifyWatchers(key);
                            }, "ZADD", (Object[]) args);
                            
                            send(out, isResp, Resp.integer(addedCount[0]), "(integer) " + addedCount[0]);
                        } catch (RuntimeException e) {
                            String msg = e.getMessage();
                            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                                send(out, isResp, Resp.error(msg), "(error) " + msg);
                            else throw e;
                        }
                }
                break;
                
            case "ZRANGE":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZRANGE key start stop [WITHSCORES]"), "(error) usage: ZRANGE key start stop [WITHSCORES]");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    boolean withScores = parts.size() > 4 && new String(parts.get(parts.size()-1), StandardCharsets.UTF_8).equalsIgnoreCase("WITHSCORES");
                    
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.ZSET) {
                        if (entry != null && entry.type != DataType.ZSET) {
                            send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                        } else {
                            send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                        }
                    } else {
                        try {
                            int start = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                            int end = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                            CaradeZSet zset = (CaradeZSet) entry.getValue();
                            int size = zset.size();
                            
                            if (start < 0) start += size;
                            if (end < 0) end += size;
                            if (start < 0) start = 0;
                            
                            List<byte[]> result = new ArrayList<>();
                            List<String> resultStr = new ArrayList<>();
                            if (start <= end) {
                                Iterator<ZNode> it = zset.sorted.iterator();
                                int idx = 0;
                                while (it.hasNext() && idx <= end) {
                                    ZNode node = it.next();
                                    if (idx >= start) {
                                        result.add(node.member.getBytes(StandardCharsets.UTF_8));
                                        resultStr.add(node.member);
                                        if (withScores) {
                                            String s = String.valueOf(node.score);
                                            if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                            result.add(s.getBytes(StandardCharsets.UTF_8));
                                            resultStr.add(s);
                                        }
                                    }
                                    idx++;
                                }
                            }
                            
                            if (isResp) send(out, true, Resp.array(result), null);
                            else {
                                    StringBuilder sb = new StringBuilder();
                                    for (int i = 0; i < resultStr.size(); i++) {
                                        sb.append((i+1) + ") \"" + resultStr.get(i) + "\"\n");
                                    }
                                    send(out, false, null, sb.toString().trim());
                            }
                            
                        } catch (NumberFormatException e) {
                            send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                        }
                    }
                }
                break;
                
            case "ZREVRANGE":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZREVRANGE key start stop [WITHSCORES]"), "(error) usage: ZREVRANGE key start stop [WITHSCORES]");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    boolean withScores = parts.size() > 4 && new String(parts.get(parts.size()-1), StandardCharsets.UTF_8).equalsIgnoreCase("WITHSCORES");
                    
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.ZSET) {
                         if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                         else send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                    } else {
                        try {
                            int start = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                            int end = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                            CaradeZSet zset = (CaradeZSet) entry.getValue();
                            int size = zset.size();
                            
                            if (start < 0) start += size;
                            if (end < 0) end += size;
                            if (start < 0) start = 0;
                            
                            List<byte[]> result = new ArrayList<>();
                            List<String> resultStr = new ArrayList<>();
                            if (start <= end) {
                                Iterator<ZNode> it = zset.sorted.descendingIterator();
                                int idx = 0;
                                while (it.hasNext() && idx <= end) {
                                    ZNode node = it.next();
                                    if (idx >= start) {
                                        result.add(node.member.getBytes(StandardCharsets.UTF_8));
                                        resultStr.add(node.member);
                                        if (withScores) {
                                            String s = String.valueOf(node.score);
                                            if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                            result.add(s.getBytes(StandardCharsets.UTF_8));
                                            resultStr.add(s);
                                        }
                                    }
                                    idx++;
                                }
                            }
                            
                            if (isResp) send(out, true, Resp.array(result), null);
                            else {
                                    StringBuilder sb = new StringBuilder();
                                    for (int i = 0; i < resultStr.size(); i++) {
                                        sb.append((i+1) + ") \"" + resultStr.get(i) + "\"\n");
                                    }
                                    send(out, false, null, sb.toString().trim());
                            }
                        } catch (NumberFormatException e) {
                            send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                        }
                    }
                }
                break;
                
            case "ZRANK":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: ZRANK key member"), "(error) usage: ZRANK key member");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String member = new String(parts.get(2), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.ZSET) {
                            if (entry != null) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                            else send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                    } else {
                        CaradeZSet zset = (CaradeZSet) entry.getValue();
                        Double score = zset.score(member);
                        if (score == null) {
                            send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                        } else {
                            // O(N) scan
                            int rank = 0;
                            for (ZNode node : zset.sorted) {
                                if (node.member.equals(member)) break;
                                rank++;
                            }
                            send(out, isResp, Resp.integer(rank), "(integer) " + rank);
                        }
                    }
                }
                break;
            
            case "ZREM":
                 if (parts.size() < 3) send(out, isResp, Resp.error("usage: ZREM key member"), "(error) usage: ZREM key member");
                 else {
                     String key = new String(parts.get(1), StandardCharsets.UTF_8);
                     String member = new String(parts.get(2), StandardCharsets.UTF_8);
                     final int[] ret = {0};
                     
                     executeWrite(() -> {
                         Carade.db.getStore(dbIndex).computeIfPresent(key, (k, v) -> {
                             if (v.type == DataType.ZSET) {
                                 CaradeZSet zset = (CaradeZSet) v.getValue();
                                 Double score = zset.scores.remove(member);
                                 if (score != null) {
                                     zset.sorted.remove(new ZNode(score, member));
                                     ret[0] = 1;
                                 }
                                 if (zset.scores.isEmpty()) return null;
                             }
                             return v;
                         });
                         if (ret[0] == 1) {
                             Carade.notifyWatchers(key);
                         }
                     }, "ZREM", key, member);

                     send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                 }
                 break;
            
            case "ZINCRBY":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZINCRBY key increment member"), "(error) usage: ZINCRBY key increment member");
                else {
                    Carade.performEvictionIfNeeded();
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String member = new String(parts.get(3), StandardCharsets.UTF_8);
                    final double[] ret = {0.0};
                    try {
                        double incr = Double.parseDouble(new String(parts.get(2), StandardCharsets.UTF_8));
                        String incrStr = new String(parts.get(2), StandardCharsets.UTF_8);
                        
                        executeWrite(() -> {
                            Carade.db.getStore(dbIndex).compute(key, (k, v) -> {
                                CaradeZSet zset;
                                if (v == null) {
                                    zset = new CaradeZSet();
                                    v = new ValueEntry(zset, DataType.ZSET, -1);
                                } else if (v.type != DataType.ZSET) {
                                    throw new RuntimeException("WRONGTYPE");
                                } else {
                                    zset = (CaradeZSet) v.getValue();
                                }
                                ret[0] = zset.incrBy(incr, member);
                                v.touch();
                                return v;
                            });
                            Carade.notifyWatchers(key);
                        }, "ZINCRBY", key, incrStr, member);
                        
                        String s = String.valueOf(ret[0]);
                        if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                        send(out, isResp, Resp.bulkString(s.getBytes(StandardCharsets.UTF_8)), s);
                    } catch (NumberFormatException e) {
                        send(out, isResp, Resp.error("ERR value is not a valid float"), "(error) ERR value is not a valid float");
                    } catch (RuntimeException e) {
                        String msg = e.getMessage();
                        if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                            send(out, isResp, Resp.error(msg), "(error) " + msg);
                        else throw e;
                    }
                }
                break;

            case "ZCARD":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: ZCARD key"), "(error) usage: ZCARD key");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.ZSET) {
                        if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                        else send(out, isResp, Resp.integer(0), "(integer) 0");
                    } else {
                        CaradeZSet zset = (CaradeZSet) entry.getValue();
                        send(out, isResp, Resp.integer(zset.size()), "(integer) " + zset.size());
                    }
                }
                break;

            case "ZCOUNT":
                if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZCOUNT key min max"), "(error) usage: ZCOUNT key min max");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.ZSET) {
                         if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                         else send(out, isResp, Resp.integer(0), "(integer) 0");
                    } else {
                         try {
                             String minStr = new String(parts.get(2), StandardCharsets.UTF_8).toLowerCase();
                             String maxStr = new String(parts.get(3), StandardCharsets.UTF_8).toLowerCase();
                             double min = minStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (minStr.equals("+inf") || minStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(minStr));
                             double max = maxStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (maxStr.equals("+inf") || maxStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(maxStr));
                             
                             CaradeZSet zset = (CaradeZSet) entry.getValue();
                             long count = 0;
                             ZNode startNode = new ZNode(min, "");
                             for (ZNode node : zset.sorted.tailSet(startNode)) {
                                 if (node.score > max) break;
                                 count++;
                             }
                             send(out, isResp, Resp.integer(count), "(integer) " + count);
                         } catch (NumberFormatException e) {
                             send(out, isResp, Resp.error("ERR min or max is not a float"), "(error) ERR min or max is not a float");
                         }
                    }
                }
                break;

            case "ZSCORE":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: ZSCORE key member"), "(error) usage: ZSCORE key member");
                else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    String member = new String(parts.get(2), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                     if (entry == null || entry.type != DataType.ZSET) {
                         if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                         else send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                     } else {
                         CaradeZSet zset = (CaradeZSet) entry.getValue();
                         Double score = zset.score(member);
                         if (score == null) send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                         else {
                             String s = String.valueOf(score);
                             if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                             send(out, isResp, Resp.bulkString(s.getBytes(StandardCharsets.UTF_8)), s);
                         }
                     }
                }
                break;
            
            case "ZRANGEBYSCORE":
            case "ZREVRANGEBYSCORE":
                if (parts.size() < 4) {
                    if (out != null) send(out, isResp, Resp.error("usage"), "error");
                    else sendError("usage: " + cmd + " key min max [WITHSCORES] [LIMIT offset count]");
                } else {
                    String key = new String(parts.get(1), StandardCharsets.UTF_8);
                    ValueEntry entry = Carade.db.get(dbIndex, key);
                    if (entry == null || entry.type != DataType.ZSET) {
                        if (entry != null && entry.type != DataType.ZSET) {
                            if (out != null) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                            else sendError("WRONGTYPE");
                        }
                        else {
                            if (out != null) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                            else sendArray(Collections.emptyList());
                        }
                    } else {
                        try {
                            // Parse min/max based on command
                            String minStr, maxStr;
                            if (cmd.equals("ZRANGEBYSCORE")) {
                                minStr = new String(parts.get(2), StandardCharsets.UTF_8).toLowerCase();
                                maxStr = new String(parts.get(3), StandardCharsets.UTF_8).toLowerCase();
                            } else {
                                maxStr = new String(parts.get(2), StandardCharsets.UTF_8).toLowerCase();
                                minStr = new String(parts.get(3), StandardCharsets.UTF_8).toLowerCase();
                            }
                            
                            boolean minExclusive = minStr.startsWith("(");
                            if (minExclusive) minStr = minStr.substring(1);
                            double min = minStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (minStr.equals("+inf") || minStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(minStr));
                            
                            boolean maxExclusive = maxStr.startsWith("(");
                            if (maxExclusive) maxStr = maxStr.substring(1);
                            double max = maxStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (maxStr.equals("+inf") || maxStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(maxStr));
                            
                            // Parse Options
                            boolean withScores = false;
                            int offset = 0;
                            int count = Integer.MAX_VALUE;
                            
                            for (int i = 4; i < parts.size(); i++) {
                                String arg = new String(parts.get(i), StandardCharsets.UTF_8).toUpperCase();
                                if (arg.equals("WITHSCORES")) {
                                    withScores = true;
                                } else if (arg.equals("LIMIT") && i + 2 < parts.size()) {
                                    offset = Integer.parseInt(new String(parts.get(++i), StandardCharsets.UTF_8));
                                    count = Integer.parseInt(new String(parts.get(++i), StandardCharsets.UTF_8));
                                }
                            }
                            
                            CaradeZSet zset = (CaradeZSet) entry.getValue();
                            NavigableSet<ZNode> subset = zset.rangeByScore(min, !minExclusive, max, !maxExclusive);
                            
                            Iterator<ZNode> it = cmd.equals("ZRANGEBYSCORE") ? subset.iterator() : subset.descendingIterator();
                            
                            // Apply LIMIT
                            int skipped = 0;
                            while (skipped < offset && it.hasNext()) {
                                it.next();
                                skipped++;
                            }
                            
                            List<byte[]> result = new ArrayList<>();
                            List<String> resultStr = new ArrayList<>();
                            int added = 0;
                            while (it.hasNext() && added < count) {
                                ZNode node = it.next();
                                result.add(node.member.getBytes(StandardCharsets.UTF_8));
                                resultStr.add(node.member);
                                if (withScores) {
                                    String s = String.valueOf(node.score);
                                    if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                    result.add(s.getBytes(StandardCharsets.UTF_8));
                                    resultStr.add(s);
                                }
                                added++;
                            }
                            
                            if (isResp) {
                                if (out != null) send(out, true, Resp.array(result), null);
                                else send(true, Resp.array(result), null);
                            } else {
                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < resultStr.size(); i++) {
                                    sb.append((i+1) + ") \"" + resultStr.get(i) + "\"\n");
                                }
                                if (out != null) send(out, false, null, sb.toString().trim());
                                else send(false, null, sb.toString().trim());
                            }
                        } catch (NumberFormatException e) {
                            if (out != null) send(out, isResp, Resp.error("ERR min or max is not a float"), "error");
                            else sendError("ERR min or max is not a float");
                        }
                    }
                }
                break;

            // --- NEW PUB/SUB COMMANDS ---
            case "SUBSCRIBE":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SUBSCRIBE channel"), "(error) usage: SUBSCRIBE channel");
                else {
                    for (int i = 1; i < parts.size(); i++) {
                        String channel = new String(parts.get(i), StandardCharsets.UTF_8);
                        Carade.pubSub.subscribe(channel, this);
                        isSubscribed = true;
                        if (isResp) {
                            List<byte[]> resp = new ArrayList<>();
                            resp.add("subscribe".getBytes(StandardCharsets.UTF_8));
                            resp.add(channel.getBytes(StandardCharsets.UTF_8));
                            resp.add("1".getBytes(StandardCharsets.UTF_8));
                            send(out, true, Resp.array(resp), null);
                        } else {
                            send(out, false, null, "Subscribed to channel: " + channel);
                        }
                    }
                }
                break;
            
            case "UNSUBSCRIBE":
                if (parts.size() < 2) {
                    // Unsubscribe all
                    Carade.pubSub.unsubscribeAll(this);
                    if (isSubscribed) {
                        isSubscribed = false; 
                        if (isResp) {
                            List<byte[]> resp = new ArrayList<>();
                            resp.add("unsubscribe".getBytes(StandardCharsets.UTF_8));
                            resp.add(null);
                            resp.add("0".getBytes(StandardCharsets.UTF_8));
                            send(out, true, Resp.array(resp), null);
                        }
                        else send(out, false, null, "Unsubscribed from all");
                    }
                } else {
                    for (int i = 1; i < parts.size(); i++) {
                        String channel = new String(parts.get(i), StandardCharsets.UTF_8);
                        Carade.pubSub.unsubscribe(channel, this);
                        if (isResp) {
                            List<byte[]> resp = new ArrayList<>();
                            resp.add("unsubscribe".getBytes(StandardCharsets.UTF_8));
                            resp.add(channel.getBytes(StandardCharsets.UTF_8));
                            resp.add("0".getBytes(StandardCharsets.UTF_8));
                            send(out, true, Resp.array(resp), null);
                        }
                        else send(out, false, null, "Unsubscribed from: " + channel);
                    }
                }
                break;

            case "PSUBSCRIBE":
                if (parts.size() < 2) send(out, isResp, Resp.error("usage: PSUBSCRIBE pattern"), "(error) usage: PSUBSCRIBE pattern");
                else {
                    for (int i = 1; i < parts.size(); i++) {
                        String pattern = new String(parts.get(i), StandardCharsets.UTF_8);
                        Carade.pubSub.psubscribe(pattern, this);
                        isSubscribed = true;
                        if (isResp) {
                            List<byte[]> resp = new ArrayList<>();
                            resp.add("psubscribe".getBytes(StandardCharsets.UTF_8));
                            resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                            resp.add("1".getBytes(StandardCharsets.UTF_8));
                            send(out, true, Resp.array(resp), null);
                        } else {
                            send(out, false, null, "Subscribed to pattern: " + pattern);
                        }
                    }
                }
                break;
                
            case "PUNSUBSCRIBE":
                // Simplified implementation
                if (parts.size() >= 2) {
                        for (int i = 1; i < parts.size(); i++) {
                        String pattern = new String(parts.get(i), StandardCharsets.UTF_8);
                        Carade.pubSub.punsubscribe(pattern, this);
                        if (isResp) {
                            List<byte[]> resp = new ArrayList<>();
                            resp.add("punsubscribe".getBytes(StandardCharsets.UTF_8));
                            resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                            resp.add("0".getBytes(StandardCharsets.UTF_8));
                            send(out, true, Resp.array(resp), null);
                        } 
                        else send(out, false, null, "Unsubscribed from pattern: " + pattern);
                        }
                }
                break;

            case "PUBLISH":
                if (parts.size() < 3) send(out, isResp, Resp.error("usage: PUBLISH channel message"), "(error) usage: PUBLISH channel message");
                else {
                    String channel = new String(parts.get(1), StandardCharsets.UTF_8);
                    String msg = new String(parts.get(2), StandardCharsets.UTF_8);
                    int count = Carade.pubSub.publish(channel, msg);
                    send(out, isResp, Resp.integer(count), "(integer) " + count);
                }
                break;

            case "INFO":
                StringBuilder info = new StringBuilder();
                info.append("# Server\n");
                info.append("carade_version:0.2.0\n");
                info.append("tcp_port:").append(Carade.config.port).append("\n");
                info.append("uptime_in_seconds:").append(ManagementFactory.getRuntimeMXBean().getUptime() / 1000).append("\n");
                info.append("\n# Clients\n");
                info.append("connected_clients:").append(Carade.activeConnections.get()).append("\n");
                info.append("\n# Memory\n");
                info.append("used_memory:").append(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).append("\n");
                info.append("maxmemory:").append(Carade.config.maxMemory).append("\n");
                info.append("\n# Stats\n");
                info.append("total_commands_processed:").append(Carade.totalCommands.get()).append("\n");
                info.append("keyspace_hits:").append(Carade.keyspaceHits.get()).append("\n");
                info.append("keyspace_misses:").append(Carade.keyspaceMisses.get()).append("\n");
                info.append("\n# Persistence\n");
                info.append("aof_enabled:1\n");
                
                if (isResp) send(out, true, Resp.bulkString(info.toString().getBytes(StandardCharsets.UTF_8)), null);
                else send(out, false, null, info.toString());
                break;
            case "DBSIZE": send(out, isResp, Resp.integer(Carade.db.size(dbIndex)), "(integer) " + Carade.db.size(dbIndex)); break;
            case "FLUSHALL": 
                executeWrite(() -> {
                    // Notify all watchers as all keys are gone
                    for (String k : Carade.watchers.keySet()) {
                        Carade.notifyWatchers(k);
                    }
                    Carade.db.clearAll(); 
                }, "FLUSHALL");
                
                send(out, isResp, Resp.simpleString("OK"), "OK"); 
                break;
            case "FLUSHDB": 
                executeWrite(() -> {
                     Carade.db.clear(dbIndex);
                }, "FLUSHDB");
                
                send(out, isResp, Resp.simpleString("OK"), "OK"); 
                break;
            case "BGREWRITEAOF":
                // Execute in background
                CompletableFuture.runAsync(() -> {
                    System.out.println("🔄 Starting Background AOF Rewrite...");
                    Carade.aofHandler.rewrite(Carade.db);
                });
                send(out, isResp, Resp.simpleString("Background append only file rewriting started"), "Background append only file rewriting started");
                break;
            case "PING": send(out, isResp, Resp.simpleString("PONG"), "PONG"); break;
            case "QUIT": ctx.close(); return;
            default: send(out, isResp, Resp.error("ERR unknown command"), "(error) ERR unknown command");
        }
    }
}
