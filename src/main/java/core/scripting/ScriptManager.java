package core.scripting;

import org.luaj.vm2.*;
import org.luaj.vm2.lib.ZeroArgFunction;
import org.luaj.vm2.lib.jse.JsePlatform;
import core.network.ClientHandler;
import core.utils.Log;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ScriptManager {
    private static final ScriptManager INSTANCE = new ScriptManager();
    private final ConcurrentHashMap<String, LuaValue> scriptCache = new ConcurrentHashMap<>();
    private final ReentrantLock executionLock = new ReentrantLock();
    
    private final Globals globals;
    private volatile boolean stopScript = false;
    private volatile boolean scriptDirty = false;

    private ScriptManager() {
        globals = JsePlatform.debugGlobals();
        // Sandbox: Remove dangerous libraries
        globals.set("luajava", LuaValue.NIL);
        globals.set("io", LuaValue.NIL);
        globals.set("os", LuaValue.NIL);
        globals.set("package", LuaValue.NIL);
    }
    
    public static ScriptManager getInstance() {
        return INSTANCE;
    }
    
    public void setScriptDirty(boolean dirty) {
        this.scriptDirty = dirty;
    }

    public void killScript() {
        if (!executionLock.isLocked()) {
             throw new RuntimeException("NOTBUSY No scripts in execution right now.");
        }
        if (scriptDirty) {
             throw new RuntimeException("UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.");
        }
        stopScript = true;
    }

    public Object eval(ClientHandler client, String script, List<String> keys, List<String> args, boolean readOnly) {
        String sha = calculateSha1(script);
        return execute(client, script, keys, args, sha, readOnly);
    }
    
    public Object evalSha(ClientHandler client, String sha, List<String> keys, List<String> args, boolean readOnly) {
        if (!scriptCache.containsKey(sha)) {
            throw new RuntimeException("NOSCRIPT No matching script. Please use EVAL.");
        }
        return execute(client, null, keys, args, sha, readOnly);
    }

    private Object execute(ClientHandler client, String script, List<String> keys, List<String> args, String sha, boolean readOnly) {
        executionLock.lock();
        try {
            stopScript = false;
            scriptDirty = false;
            
            // Prepare KEYS and ARGV
            LuaTable keysTable = new LuaTable();
            for (int i = 0; i < keys.size(); i++) {
                keysTable.set(i + 1, LuaValue.valueOf(keys.get(i)));
            }
            globals.set("KEYS", keysTable);
            
            LuaTable argvTable = new LuaTable();
            for (int i = 0; i < args.size(); i++) {
                argvTable.set(i + 1, LuaValue.valueOf(args.get(i)));
            }
            globals.set("ARGV", argvTable);
            
            // Register 'redis' lib with current client
            globals.load(new RedisLuaBinding(client, readOnly)); // This sets 'redis' global
            
            // Install instruction hook for SCRIPT KILL
            try {
                globals.get("debug").get("sethook").call(
                    new ZeroArgFunction() {
                        @Override
                        public LuaValue call() {
                            if (stopScript) {
                                throw new LuaError("Script killed by user with SCRIPT KILL...");
                            }
                            return NIL;
                        }
                    },
                    LuaValue.valueOf(""), // line hook
                    LuaValue.valueOf(1000) // check every 1000 instructions
                );
            } catch (Exception e) {
                Log.error("ScriptManager: Failed to set hook: " + e.getMessage());
            }
            
            LuaValue chunk;
            if (scriptCache.containsKey(sha)) {
                chunk = scriptCache.get(sha);
            } else {
                 if (script == null) throw new RuntimeException("NOSCRIPT No matching script. Please use EVAL.");
                 // Compile
                 chunk = globals.load(script, "script");
                 scriptCache.put(sha, chunk);
            }
            
            try {
                LuaValue result = chunk.call();
                return LuaConverter.toJava(result);
            } catch (LuaError e) {
                // Check if it was our kill signal
                if (e.getMessage().contains("Script killed by user")) {
                    throw new RuntimeException("ERR Script killed by user with SCRIPT KILL...");
                }
                throw new RuntimeException("ERR Error running script (call to " + sha + "): " + e.getMessage());
            } finally {
                // Uninstall hook
                try {
                    globals.get("debug").get("sethook").call();
                } catch (Exception e) {}
            }
        } finally {
            executionLock.unlock();
        }
    }
    
    public String load(String script) {
        String sha = calculateSha1(script);
        if (!scriptCache.containsKey(sha)) {
            LuaValue chunk = globals.load(script, "script");
            scriptCache.put(sha, chunk);
        }
        return sha;
    }
    
    public boolean exists(String sha) {
        return scriptCache.containsKey(sha);
    }
    
    public void flush() {
        scriptCache.clear();
    }

    private String calculateSha1(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] bytes = md.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    }
}
