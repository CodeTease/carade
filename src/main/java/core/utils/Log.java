package core.utils;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public class Log {
    private static final Logger logger = Logger.getLogger("Carade");

    static {
        logger.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter() {
            @Override
            public synchronized String format(LogRecord record) {
                // If it's an error, we might want to print to stderr, but ConsoleHandler prints to stderr by default.
                // Let's just format it cleanly.
                String levelStr = record.getLevel() == Level.SEVERE ? "ERROR" : 
                                  record.getLevel() == Level.WARNING ? "WARN" : 
                                  record.getLevel() == Level.INFO ? "INFO" : "DEBUG";
                                  
                return String.format("[%s] %s%n", levelStr, record.getMessage());
            }
        });
        logger.addHandler(handler);
        logger.setLevel(Level.INFO);
    }

    public static void info(String msg) {
        logger.info(msg);
    }

    public static void warn(String msg) {
        logger.warning(msg);
    }

    public static void error(String msg) {
        logger.severe(msg);
    }
    
    public static void debug(String msg) {
        logger.fine(msg);
    }
}
