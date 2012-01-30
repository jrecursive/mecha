package mecha.db;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.*;

import mecha.Mecha;
import mecha.util.*;
import mecha.json.*;

public class EventLogManager {
    final private static Logger log = 
        Logger.getLogger(EventLogManager.class.getName());
    
    final private Map<String, EventLog> eventLogs;
    
    public EventLogManager() {
        eventLogs = new ConcurrentHashMap<String, EventLog>();
    }
    
    public synchronized EventLog createEventLogForWrite(String eventLogName, 
                                                        String eventLogFilename) throws Exception {
        EventLog eventLog = eventLogs.get(eventLogName);
        if (eventLog == null) {
            log.info(eventLogFilename);
            File f = new File(eventLogFilename);
            if (f.exists()) {
                throw new Exception(eventLogFilename + " already exists!");
            }
            eventLog = new EventLog(eventLogName, eventLogFilename, getEventLogMode());
            log.info(eventLogName + ": " + eventLogFilename + " opened");
            eventLogs.put(eventLogName, eventLog);
        } else {
            throw new Exception ("eventLog already open!");
        }
        return eventLog;
    }
    
    public EventLog getEventLog(String eventLogName) throws Exception {
        return eventLogs.get(eventLogName);
    }
    
    public static String getEventLogBasePath() throws Exception {
        return Mecha.getConfig().getString("event-log-directory");
    }

    public static String getEventLogMode() throws Exception {
        return Mecha.getConfig().getString("event-log-mode");
    }
    
    public synchronized void shutdown() throws Exception {
        for(String eventLogName : eventLogs.keySet()) {
            EventLog eventLog = eventLogs.get(eventLogName);
            log.info("shutdown: " + eventLogName + ": " + eventLog.getFilename() + ": close");
            eventLog.close();
        }
        eventLogs.clear();
        log.info("event logs shutdown");
    }
    
    public synchronized void recycle() throws Exception {
        log.info("recycling event logs...");
        for(String eventLogName : eventLogs.keySet()) {
            EventLog eventLog = eventLogs.get(eventLogName);
            log.info("shutdown: " + eventLogName + ": " + eventLog.getFilename() + ": recycle");
            eventLog.recycle();
        }
        log.info("event logs recycled");
    }
}
