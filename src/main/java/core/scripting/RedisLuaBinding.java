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
    private final boolean readOnly;

    public RedisLuaBinding(ClientHandler client, boolean readOnly) {
        this.client = client;
        this.readOnly = readOnly;
    }
    
    public RedisLuaBinding(ClientHandler client) {
        this(client, false);
    }

    @Override
    public LuaValue call(LuaValue modname, LuaValue env) {
        LuaTable redis = new LuaTable();
        redis.set("call", new RedisCall(client, false, readOnly));
        redis.set("pcall", new RedisCall(client, true, readOnly));
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
        private final boolean readOnly;

        public RedisCall(ClientHandler client, boolean pcall, boolean readOnly) {
            this.client = client;
            this.pcall = pcall;
            this.readOnly = readOnly;
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
                
                boolean isWrite = client.isWriteCommand(cmdName);
                if (readOnly && isWrite) {
                    throw new RuntimeException("ERR Write commands not allowed in read-only mode");
                }
                
                if (!readOnly && isWrite) {
                     ScriptManager.getInstance().setScriptDirty(true);
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
