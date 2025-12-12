import java.io.*;
import java.util.*;

public class AofHandler {
    private final String filename;
    private PrintWriter writer;
    
    public AofHandler(String filename) {
        this.filename = filename;
        try {
            // Append mode
            this.writer = new PrintWriter(new FileWriter(filename, true), true); 
        } catch (IOException e) {
            System.err.println("⚠️ Could not open AOF file: " + e.getMessage());
        }
    }

    public synchronized void log(String cmd, String... args) {
        if (writer == null) return;
        List<String> parts = new ArrayList<>();
        parts.add(cmd);
        parts.addAll(Arrays.asList(args));
        String resp = Resp.array(parts);
        writer.print(resp);
        writer.flush();
    }
    
    public void replay(java.util.function.Consumer<List<String>> commandExecutor) {
        File f = new File(filename);
        if (!f.exists()) return;
        
        System.out.println("📂 Replaying AOF...");
        try (FileInputStream fis = new FileInputStream(f)) {
            while (true) {
                Resp.Request req = Resp.parse(fis);
                if (req == null) break;
                
                if (!req.args.isEmpty()) {
                    commandExecutor.accept(req.args);
                }
            }
        } catch (IOException e) {
            System.err.println("⚠️ Error replaying AOF: " + e.getMessage());
        }
    }
    
    public void close() {
        if (writer != null) writer.close();
    }
}
