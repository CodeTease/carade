package core.scripting;

import org.luaj.vm2.*;
import org.luaj.vm2.lib.jse.JseBaseLib;
import org.luaj.vm2.lib.jse.JseMathLib;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.TableLib;
import org.luaj.vm2.lib.StringLib;
import org.luaj.vm2.lib.DebugLib;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ScriptManager {
    private static final ScriptManager INSTANCE = new ScriptManager();
    private final ConcurrentHashMap<String, LuaValue> scriptCache = new ConcurrentHashMap<>();
    
    private final Globals globals;

    private ScriptManager() {
        globals = new Globals();
        globals.load(new JseBaseLib());
        globals.load(new TableLib());
        globals.load(new StringLib());
        globals.load(new JseMathLib());
        globals.load(new DebugLib());
        LoadState.install(globals);
        org.luaj.vm2.compiler.LuaC.install(globals);
    }
    
    public static ScriptManager getInstance() {
        return INSTANCE;
    }

    public Object eval(ClientHandler client, String script, List<String> keys, List<String> args) {
        String sha = calculateSha1(script);
        return execute(client, script, keys, args, sha);
    }
    
    public Object evalSha(ClientHandler client, String sha, List<String> keys, List<String> args) {
        if (!scriptCache.containsKey(sha)) {
            throw new RuntimeException("NOSCRIPT No matching script. Please use EVAL.");
        }
        return execute(client, null, keys, args, sha);
    }

    private synchronized Object execute(ClientHandler client, String script, List<String> keys, List<String> args, String sha) {
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
        // Since we are synchronized (and under global write lock), we can just replace the 'redis' global.
        globals.load(new RedisLuaBinding(client)); // This sets 'redis' global
        
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
            throw new RuntimeException("ERR Error running script (call to " + sha + "): " + e.getMessage());
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
