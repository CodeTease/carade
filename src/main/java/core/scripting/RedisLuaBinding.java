package core.scripting;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.LibFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import core.commands.Command;
import core.commands.CommandRegistry;
import core.network.ClientHandler;

import org.luaj.vm2.LuaString;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RedisLuaBinding extends TwoArgFunction {

    private final ClientHandler client;

    public RedisLuaBinding(ClientHandler client) {
        this.client = client;
    }

    @Override
    public LuaValue call(LuaValue modname, LuaValue env) {
        LuaTable redis = new LuaTable();
        redis.set("call", new RedisCall(client, false));
        redis.set("pcall", new RedisCall(client, true));
        redis.set("status_reply", new StatusReply());
        redis.set("error_reply", new ErrorReply());
        // redis.set("sha1hex", new Sha1Hex()); // If needed
        // redis.set("log", new Log()); // If needed
        env.set("redis", redis);
        return redis;
    }
    
    // StatusReply and ErrorReply helpers
    static class StatusReply extends org.luaj.vm2.lib.OneArgFunction {
        @Override
        public LuaValue call(LuaValue arg) {
            LuaTable t = new LuaTable();
            t.set("ok", arg);
            return t;
        }
    }
    
    static class ErrorReply extends org.luaj.vm2.lib.OneArgFunction {
        @Override
        public LuaValue call(LuaValue arg) {
            LuaTable t = new LuaTable();
            t.set("err", arg);
            return t;
        }
    }

    static class RedisCall extends VarargsFunction {
        private final ClientHandler client;
        private final boolean pcall;

        public RedisCall(ClientHandler client, boolean pcall) {
            this.client = client;
            this.pcall = pcall;
        }

        @Override
        public Varargs invoke(Varargs args) {
            try {
                // 1. Parse arguments
                int n = args.narg();
                if (n < 1) {
                    throw new RuntimeException("Please specify at least one argument for redis.call()");
                }
                
                String cmdName = args.arg(1).checkjstring().toUpperCase();
                Command command = CommandRegistry.get(cmdName);
                if (command == null) {
                    throw new RuntimeException("Unknown command '" + cmdName + "'");
                }
                
                // Prepare args for command execution (List<byte[]>)
                List<byte[]> cmdArgs = new ArrayList<>(n);
                for (int i = 1; i <= n; i++) {
                     LuaValue arg = args.arg(i);
                     if (arg.isstring()) {
                         LuaString s = arg.checkstring();
                         byte[] b = new byte[s.m_length];
                         s.copyInto(0, b, 0, s.m_length);
                         cmdArgs.add(b);
                     } else {
                         String s = arg.toString();
                         cmdArgs.add(s.getBytes(StandardCharsets.UTF_8));
                     }
                }
                
                // 2. Setup capture buffer
                ByteArrayOutputStream capture = new ByteArrayOutputStream();
                client.setCaptureBuffer(capture);
                
                // 3. Execute command
                // Note: We need to handle locking?
                // The EVAL command itself holds the lock.
                // If EVAL holds Write Lock, we can execute anything.
                // If EVAL holds Read Lock (EVAL_RO/EVAL_SHA_RO - not implemented yet), we can only execute reads.
                // Assuming EVAL holds Write Lock for now as per design.
                // However, Command.execute calls client methods which might check locks?
                // No, Command.execute usually just runs logic. 
                // But some commands like SetCommand call client.executeWrite() which adds to AOF/Replication.
                // client.executeWrite() uses WriteSequencer.
                // WriteSequencer is thread-safe.
                
                // Important: We need to ensure the command doesn't write to network directly.
                // We set captureBuffer, so client.send() writes to it.
                
                try {
                    command.execute(client, cmdArgs);
                } finally {
                    client.setCaptureBuffer(null);
                }
                
                // 4. Parse result
                byte[] output = capture.toByteArray();
                Object result = RespParser.parse(output);
                
                // 5. Convert to Lua
                if (result instanceof RespParser.RespError) {
                     if (pcall) {
                         LuaTable err = new LuaTable();
                         err.set("err", LuaValue.valueOf(((RespParser.RespError) result).message));
                         return err;
                     } else {
                         // redis.call raises error
                         throw new RuntimeException(((RespParser.RespError) result).message);
                     }
                }
                
                return LuaConverter.toLua(result);
                
            } catch (Exception e) {
                if (pcall) {
                    LuaTable err = new LuaTable();
                    err.set("err", LuaValue.valueOf(e.getMessage()));
                    return err;
                } else {
                    throw new LuaError(e.getMessage());
                }
            }
        }
    }
    
    // Abstract class implementation provided by Luaj
    public abstract static class VarargsFunction extends LibFunction {
        public LuaValue call() {
            return invoke(LuaValue.NONE).arg1();
        }
        public LuaValue call(LuaValue arg) {
            return invoke(arg).arg1();
        }
        public LuaValue call(LuaValue arg1, LuaValue arg2) {
            return invoke(LuaValue.varargsOf(arg1, arg2)).arg1();
        }
        public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
            return invoke(LuaValue.varargsOf(new LuaValue[]{arg1, arg2, arg3})).arg1();
        }
        public Varargs invoke(Varargs args) {
            return LuaValue.NIL;
        }
    }
}
