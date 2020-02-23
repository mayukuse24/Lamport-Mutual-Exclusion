package app;

import java.util.logging.*;

/**
 * Logger class
 */
public class Applog {
    public static void init() {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$-7s] %5$s %n");

        Logger l = Logger.getLogger(Applog.class.getName());

        String level = System.getenv("LOG_LEVEL") == null ? "INFO" : System.getenv("LOG_LEVEL");
              
        l.setLevel(Level.parse(level));
    }
}